use clap::{ArgGroup, Parser, ValueEnum};
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{self, IsTerminal};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use trama::{build_file_index, collect_markdown_files, extract_wikilinks, validate_vault_path};
use unicase::UniCase;

#[derive(Parser)]
#[command(name = "hypha", version, about = "Obsidian vault link graph traverser")]
#[command(group(ArgGroup::new("mode").required(true).args(["from", "path_flag", "suggest"])))]
struct Cli {
    /// Vault root directory
    path: PathBuf,
    #[arg(long, group = "mode", value_name = "NOTE")]
    from: Option<String>,
    #[arg(long = "path", group = "mode", num_args = 2, value_names = ["FROM", "TO"])]
    path_flag: Option<Vec<String>>,
    #[arg(
        long,
        group = "mode",
        value_name = "NOTE",
        help = "suggest notes that should link to/from NOTE (co-citation ranking)"
    )]
    suggest: Option<String>,
    #[arg(long, default_value_t = 1)]
    depth: usize,
    #[arg(
        long,
        default_value_t = 15,
        help = "max suggestions to show (default 15)"
    )]
    top: usize,
    #[arg(long)]
    exclude: Vec<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Human)]
    format: OutputFormat,
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    Human,
    Json,
}

#[derive(Serialize)]
struct NeighborhoodReport {
    note: String,
    depth: usize,
    levels: Vec<DepthLevel>,
}

#[derive(Serialize)]
struct DepthLevel {
    depth: usize,
    outgoing: Vec<String>,
    incoming: Vec<String>,
}

#[derive(Serialize)]
struct PathReport {
    from: String,
    to: String,
    hops: usize,
    path: Vec<String>,
}

#[derive(Serialize)]
struct SuggestReport {
    note: String,
    suggestions: Vec<Suggestion>,
}

#[derive(Serialize)]
struct Suggestion {
    note: String,
    common_neighbors: usize,
}

/// Returns true for calendrical notes (YYYY-MM-DD daily, YYYY-WXX weekly).
/// These are temporal hubs — linked from many notes written on the same day —
/// and produce false-positive co-citation signal.
fn is_calendrical(path: &Path) -> bool {
    let stem = match path.file_stem().and_then(|s| s.to_str()) {
        Some(s) => s.to_owned(),
        None => return false,
    };
    let stem = stem.as_str();
    let b = stem.as_bytes();
    // YYYY-MM-DD
    if b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b[..4].iter().all(|c| c.is_ascii_digit())
        && b[5..7].iter().all(|c| c.is_ascii_digit())
        && b[8..].iter().all(|c| c.is_ascii_digit())
    {
        return true;
    }
    // YYYY-WXX (e.g. 2026-W09)
    if (b.len() == 7 || b.len() == 8)
        && b[4] == b'-'
        && b[5] == b'W'
        && b[..4].iter().all(|c| c.is_ascii_digit())
        && b[6..].iter().all(|c| c.is_ascii_digit())
    {
        return true;
    }
    false
}

/// Co-citation ranking: notes not yet connected to `seed` that share the most
/// common neighbors (outgoing ∪ incoming). Returns (path, overlap) sorted
/// descending, filtered to overlap >= 2, capped at `top`.
///
/// Calendrical notes (YYYY-MM-DD, YYYY-WXX) are excluded from the neighbor
/// set — they are temporal hubs, not semantic connections.
fn suggest_links(
    seed: &PathBuf,
    outgoing: &HashMap<PathBuf, Vec<PathBuf>>,
    incoming: &HashMap<PathBuf, Vec<PathBuf>>,
    top: usize,
) -> Vec<(PathBuf, usize)> {
    // Seed's full neighbor set (both directions), calendrical notes stripped.
    let seed_neighbors: HashSet<&PathBuf> = outgoing
        .get(seed)
        .into_iter()
        .flatten()
        .chain(incoming.get(seed).into_iter().flatten())
        .filter(|p| !is_calendrical(p))
        .collect();

    // Already-connected set: seed + its neighbors (skip these as suggestions).
    let mut connected: HashSet<&PathBuf> = seed_neighbors.clone();
    connected.insert(seed);

    let mut scores: Vec<(PathBuf, usize)> = outgoing
        .keys()
        .filter(|note| !connected.contains(note) && !is_calendrical(note))
        .map(|note| {
            let note_neighbors: HashSet<&PathBuf> = outgoing
                .get(note)
                .into_iter()
                .flatten()
                .chain(incoming.get(note).into_iter().flatten())
                .collect();
            let overlap = seed_neighbors.intersection(&note_neighbors).count();
            (note.clone(), overlap)
        })
        .filter(|(_, overlap)| *overlap >= 2)
        .collect();

    scores.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    scores.truncate(top);
    scores
}

fn print_suggestions(seed: &Path, suggestions: &[(PathBuf, usize)]) {
    let use_color = io::stdout().is_terminal();
    let seed_name = seed.file_stem().unwrap_or_default().to_string_lossy();
    let heading = format!("=== Suggested links for: {seed_name} ===");
    if use_color {
        println!("\x1b[1;36m{heading}\x1b[0m");
    } else {
        println!("{heading}");
    }
    if suggestions.is_empty() {
        println!("\n  No suggestions (no notes with 2+ common neighbors).");
        return;
    }
    let mut current_overlap = usize::MAX;
    for (path, overlap) in suggestions {
        if *overlap != current_overlap {
            current_overlap = *overlap;
            let noun = if *overlap == 1 {
                "neighbor"
            } else {
                "neighbors"
            };
            println!("\nCommon {noun}: {overlap}");
        }
        let name = path.file_stem().unwrap_or_default().to_string_lossy();
        println!("  {name}");
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let vault_root = match validate_vault_path(&cli.path) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Fatal error: {e}");
            return ExitCode::from(2);
        }
    };

    let files = collect_markdown_files(&vault_root, &cli.exclude);
    let index = build_file_index(&files);
    let (outgoing, incoming) = build_graph(&files, &index);

    let format = cli.format;

    if let Some(note_name) = &cli.from {
        let seed = match resolve_note(note_name, &index) {
            Ok(p) => p,
            Err(candidates) => {
                if candidates.is_empty() {
                    eprintln!("Note not found: {note_name}");
                } else {
                    eprintln!("Ambiguous: {note_name:?} matches multiple notes:");
                    for c in &candidates {
                        eprintln!("  {c}");
                    }
                }
                return ExitCode::from(1);
            }
        };

        let depth = cli.depth;
        let levels = neighborhood(&seed, &outgoing, &incoming, depth);

        match format {
            OutputFormat::Human => print_neighborhood(&seed, depth, &levels),
            OutputFormat::Json => {
                let note_name = seed
                    .file_stem()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned();
                let report = NeighborhoodReport {
                    note: note_name,
                    depth,
                    levels: levels
                        .iter()
                        .enumerate()
                        .map(|(i, (out, inc))| DepthLevel {
                            depth: i + 1,
                            outgoing: out
                                .iter()
                                .map(|p| {
                                    p.file_stem()
                                        .unwrap_or_default()
                                        .to_string_lossy()
                                        .into_owned()
                                })
                                .collect(),
                            incoming: inc
                                .iter()
                                .map(|p| {
                                    p.file_stem()
                                        .unwrap_or_default()
                                        .to_string_lossy()
                                        .into_owned()
                                })
                                .collect(),
                        })
                        .collect(),
                };
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            }
        }
        return ExitCode::SUCCESS;
    }

    if let Some(path_vals) = &cli.path_flag {
        let from_name = &path_vals[0];
        let to_name = &path_vals[1];

        let from = match resolve_note(from_name, &index) {
            Ok(p) => p,
            Err(candidates) => {
                if candidates.is_empty() {
                    eprintln!("Note not found: {from_name}");
                } else {
                    eprintln!("Ambiguous: {from_name:?}");
                    for c in &candidates {
                        eprintln!("  {c}");
                    }
                }
                return ExitCode::from(1);
            }
        };
        let to = match resolve_note(to_name, &index) {
            Ok(p) => p,
            Err(candidates) => {
                if candidates.is_empty() {
                    eprintln!("Note not found: {to_name}");
                } else {
                    eprintln!("Ambiguous: {to_name:?}");
                    for c in &candidates {
                        eprintln!("  {c}");
                    }
                }
                return ExitCode::from(1);
            }
        };

        match shortest_path(&from, &to, &outgoing) {
            Some(path) => {
                match format {
                    OutputFormat::Human => print_path(&path),
                    OutputFormat::Json => {
                        let report = PathReport {
                            from: from
                                .file_stem()
                                .unwrap_or_default()
                                .to_string_lossy()
                                .into_owned(),
                            to: to
                                .file_stem()
                                .unwrap_or_default()
                                .to_string_lossy()
                                .into_owned(),
                            hops: path.len() - 1,
                            path: path
                                .iter()
                                .map(|p| {
                                    p.file_stem()
                                        .unwrap_or_default()
                                        .to_string_lossy()
                                        .into_owned()
                                })
                                .collect(),
                        };
                        println!("{}", serde_json::to_string_pretty(&report).unwrap());
                    }
                }
                return ExitCode::SUCCESS;
            }
            None => {
                eprintln!("No directed path found from {from_name:?} to {to_name:?}");
                return ExitCode::from(1);
            }
        }
    }

    if let Some(note_name) = &cli.suggest {
        let seed = match resolve_note(note_name, &index) {
            Ok(p) => p,
            Err(candidates) => {
                if candidates.is_empty() {
                    eprintln!("Note not found: {note_name}");
                } else {
                    eprintln!("Ambiguous: {note_name:?} matches multiple notes:");
                    for c in &candidates {
                        eprintln!("  {c}");
                    }
                }
                return ExitCode::from(1);
            }
        };

        let suggestions = suggest_links(&seed, &outgoing, &incoming, cli.top);

        match format {
            OutputFormat::Human => print_suggestions(&seed, &suggestions),
            OutputFormat::Json => {
                let report = SuggestReport {
                    note: seed
                        .file_stem()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .into_owned(),
                    suggestions: suggestions
                        .iter()
                        .map(|(p, overlap)| Suggestion {
                            note: p
                                .file_stem()
                                .unwrap_or_default()
                                .to_string_lossy()
                                .into_owned(),
                            common_neighbors: *overlap,
                        })
                        .collect(),
                };
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            }
        }
        return ExitCode::SUCCESS;
    }

    ExitCode::SUCCESS
}

fn build_graph(
    files: &[PathBuf],
    index: &HashMap<UniCase<String>, PathBuf>,
) -> (
    HashMap<PathBuf, Vec<PathBuf>>,
    HashMap<PathBuf, Vec<PathBuf>>,
) {
    let mut outgoing: HashMap<PathBuf, Vec<PathBuf>> = files
        .iter()
        .cloned()
        .map(|path| (path, Vec::new()))
        .collect();
    let mut incoming: HashMap<PathBuf, Vec<PathBuf>> = files
        .iter()
        .cloned()
        .map(|path| (path, Vec::new()))
        .collect();

    for source in files {
        let Ok(content) = fs::read_to_string(source) else {
            continue;
        };
        let (links, _embeds) = extract_wikilinks(&content);

        for target_name in links {
            if let Some(target_path) = index.get(&UniCase::new(target_name)) {
                let target = target_path.clone();
                outgoing
                    .entry(source.clone())
                    .or_default()
                    .push(target.clone());
                incoming.entry(target).or_default().push(source.clone());
            }
        }
    }

    (outgoing, incoming)
}

fn resolve_note(
    query: &str,
    index: &HashMap<UniCase<String>, PathBuf>,
) -> Result<PathBuf, Vec<String>> {
    let key = UniCase::new(query.to_owned());
    if let Some(path) = index.get(&key) {
        return Ok(path.clone());
    }

    let query_lower = query.to_ascii_lowercase();
    let mut candidates: Vec<String> = index
        .keys()
        .map(|k| k.as_ref().to_string())
        .filter(|name| name.to_ascii_lowercase().contains(&query_lower))
        .collect();
    candidates.sort();

    Err(candidates)
}

fn neighborhood(
    seed: &Path,
    outgoing: &HashMap<PathBuf, Vec<PathBuf>>,
    incoming: &HashMap<PathBuf, Vec<PathBuf>>,
    depth: usize,
) -> Vec<(Vec<PathBuf>, Vec<PathBuf>)> {
    let mut seen: HashSet<PathBuf> = HashSet::new();
    seen.insert(seed.to_path_buf());

    let mut frontier: Vec<PathBuf> = vec![seed.to_path_buf()];
    let mut levels = Vec::with_capacity(depth);

    for _ in 1..=depth {
        let mut frontier_out_set: HashSet<PathBuf> = HashSet::new();
        let mut frontier_in_set: HashSet<PathBuf> = HashSet::new();

        for node in &frontier {
            for next in outgoing.get(node).map(Vec::as_slice).unwrap_or(&[]) {
                if !seen.contains(next) {
                    frontier_out_set.insert(next.clone());
                }
            }
            for prev in incoming.get(node).map(Vec::as_slice).unwrap_or(&[]) {
                if !seen.contains(prev) {
                    frontier_in_set.insert(prev.clone());
                }
            }
        }

        let mut frontier_out: Vec<PathBuf> = frontier_out_set.into_iter().collect();
        let mut frontier_in: Vec<PathBuf> = frontier_in_set.into_iter().collect();
        frontier_out.sort();
        frontier_in.sort();

        for node in &frontier_out {
            seen.insert(node.clone());
        }
        for node in &frontier_in {
            seen.insert(node.clone());
        }

        let mut next_frontier_set: HashSet<PathBuf> = HashSet::new();
        for node in &frontier_out {
            next_frontier_set.insert(node.clone());
        }
        for node in &frontier_in {
            next_frontier_set.insert(node.clone());
        }
        frontier = next_frontier_set.into_iter().collect();
        frontier.sort();

        levels.push((frontier_out, frontier_in));
    }

    levels
}

fn shortest_path(
    from: &Path,
    to: &PathBuf,
    outgoing: &HashMap<PathBuf, Vec<PathBuf>>,
) -> Option<Vec<PathBuf>> {
    let mut queue: VecDeque<PathBuf> = VecDeque::new();
    let mut parent: HashMap<PathBuf, PathBuf> = HashMap::new();
    let mut visited: HashSet<PathBuf> = HashSet::new();

    queue.push_back(from.to_path_buf());
    visited.insert(from.to_path_buf());

    while let Some(node) = queue.pop_front() {
        if &node == to {
            let mut path = vec![node.clone()];
            let mut cur = node;
            while let Some(prev) = parent.get(&cur) {
                path.push(prev.clone());
                cur = prev.clone();
            }
            path.reverse();
            return Some(path);
        }

        for next in outgoing.get(&node).map(Vec::as_slice).unwrap_or(&[]) {
            if visited.insert(next.clone()) {
                parent.insert(next.clone(), node.clone());
                queue.push_back(next.clone());
            }
        }
    }

    None
}

fn print_neighborhood(seed: &Path, depth: usize, levels: &[(Vec<PathBuf>, Vec<PathBuf>)]) {
    let use_color = io::stdout().is_terminal();
    let note = seed
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();

    let heading_text = if depth <= 1 {
        format!("=== {note} ===")
    } else {
        format!("=== {note} (depth {depth}) ===")
    };
    if use_color {
        println!("\x1b[1;36m{heading_text}\x1b[0m");
    } else {
        println!("{heading_text}");
    }
    println!();

    if depth <= 1 {
        let empty_out: Vec<PathBuf> = Vec::new();
        let empty_in: Vec<PathBuf> = Vec::new();
        let (out, inc) = levels
            .first()
            .map(|(o, i)| (o, i))
            .unwrap_or((&empty_out, &empty_in));
        println!("Outgoing ({}):", out.len());
        for p in out {
            println!("  {}", display_note_name(p));
        }
        println!();
        println!("Incoming ({}):", inc.len());
        for p in inc {
            println!("  {}", display_note_name(p));
        }
        return;
    }

    for (i, (out, inc)) in levels.iter().enumerate() {
        let section = format!("── Depth {} ──", i + 1);
        if use_color {
            println!("\x1b[1m{section}\x1b[0m");
        } else {
            println!("{section}");
        }

        let out_names = join_note_names(out);
        let in_names = join_note_names(inc);

        if i == 0 {
            println!("Outgoing ({}):  {}", out.len(), out_names);
            println!("Incoming ({}):  {}", inc.len(), in_names);
        } else {
            println!("Outgoing, new ({}):  {}", out.len(), out_names);
            println!("Incoming, new ({}):  {}", inc.len(), in_names);
        }
        println!();
    }
}

fn print_path(path: &[PathBuf]) {
    let use_color = io::stdout().is_terminal();
    let names: Vec<String> = path.iter().map(|p| display_note_name(p)).collect();
    let hops = path.len().saturating_sub(1);
    let heading_text = format!(
        "=== Path: {} → {} ({} hops) ===",
        names.first().cloned().unwrap_or_default(),
        names.last().cloned().unwrap_or_default(),
        hops
    );

    if use_color {
        println!("\x1b[1;36m{heading_text}\x1b[0m");
    } else {
        println!("{heading_text}");
    }
    println!("  {}", names.join(" → "));
}

fn display_note_name(path: &Path) -> String {
    path.file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned()
}

fn join_note_names(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .map(|p| display_note_name(p))
        .collect::<Vec<_>>()
        .join("  ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pb(path: &str) -> PathBuf {
        PathBuf::from(path)
    }

    #[test]
    fn resolve_note_exact() {
        let files = vec![pb("/vault/foo.md")];
        let index = build_file_index(&files);

        let result = resolve_note("foo", &index);
        assert_eq!(result, Ok(pb("/vault/foo.md")));
    }

    #[test]
    fn resolve_note_ambiguous() {
        let files_with_exact = vec![
            pb("/vault/capco.md"),
            pb("/vault/capco-transition.md"),
            pb("/vault/project-capco-notes.md"),
        ];
        let index_with_exact = build_file_index(&files_with_exact);
        assert_eq!(
            resolve_note("capco", &index_with_exact),
            Ok(pb("/vault/capco.md"))
        );

        let files_no_exact = vec![
            pb("/vault/capco-transition.md"),
            pb("/vault/project-capco-notes.md"),
        ];
        let index_no_exact = build_file_index(&files_no_exact);
        let err = resolve_note("capco", &index_no_exact).unwrap_err();
        assert_eq!(err, vec!["capco-transition", "project-capco-notes"]);
    }

    #[test]
    fn neighborhood_depth1() {
        let a = pb("/tmp/A.md");
        let b = pb("/tmp/B.md");
        let c = pb("/tmp/C.md");

        let mut outgoing: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        outgoing.insert(a.clone(), vec![b.clone()]);
        outgoing.insert(b.clone(), vec![]);
        outgoing.insert(c.clone(), vec![a.clone()]);

        let mut incoming: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        incoming.insert(a.clone(), vec![c.clone()]);
        incoming.insert(b.clone(), vec![a.clone()]);
        incoming.insert(c.clone(), vec![]);

        let levels = neighborhood(&a, &outgoing, &incoming, 1);
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].0, vec![b]);
        assert_eq!(levels[0].1, vec![c]);
    }

    #[test]
    fn shortest_path_found() {
        let a = pb("/tmp/A.md");
        let b = pb("/tmp/B.md");
        let c = pb("/tmp/C.md");

        let mut outgoing: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        outgoing.insert(a.clone(), vec![b.clone()]);
        outgoing.insert(b.clone(), vec![c.clone()]);
        outgoing.insert(c.clone(), vec![]);

        let path = shortest_path(&a, &c, &outgoing);
        assert_eq!(path, Some(vec![a, b, c]));
    }

    #[test]
    fn shortest_path_none() {
        let a = pb("/tmp/A.md");
        let b = pb("/tmp/B.md");

        let mut outgoing: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        outgoing.insert(a.clone(), vec![]);
        outgoing.insert(b.clone(), vec![]);

        let path = shortest_path(&a, &b, &outgoing);
        assert_eq!(path, None);
    }
}
