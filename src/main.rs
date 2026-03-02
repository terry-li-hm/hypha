use clap::{ArgGroup, Parser, ValueEnum};
use regex::Regex;
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::OsStr;
use std::fs;
use std::io::{self, IsTerminal};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::OnceLock;
use unicase::UniCase;
use walkdir::WalkDir;

#[derive(Parser)]
#[command(name = "hypha", version, about = "Obsidian vault link graph traverser")]
#[command(group(ArgGroup::new("mode").required(true).args(["from", "path_flag"])))]
struct Cli {
    /// Vault root directory
    path: PathBuf,
    #[arg(long, group = "mode", value_name = "NOTE")]
    from: Option<String>,
    #[arg(long = "path", group = "mode", num_args = 2, value_names = ["FROM", "TO"])]
    path_flag: Option<Vec<String>>,
    #[arg(long, default_value_t = 1)]
    depth: usize,
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
    seed: &PathBuf,
    outgoing: &HashMap<PathBuf, Vec<PathBuf>>,
    incoming: &HashMap<PathBuf, Vec<PathBuf>>,
    depth: usize,
) -> Vec<(Vec<PathBuf>, Vec<PathBuf>)> {
    let mut seen: HashSet<PathBuf> = HashSet::new();
    seen.insert(seed.clone());

    let mut frontier: Vec<PathBuf> = vec![seed.clone()];
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
    from: &PathBuf,
    to: &PathBuf,
    outgoing: &HashMap<PathBuf, Vec<PathBuf>>,
) -> Option<Vec<PathBuf>> {
    let mut queue: VecDeque<PathBuf> = VecDeque::new();
    let mut parent: HashMap<PathBuf, PathBuf> = HashMap::new();
    let mut visited: HashSet<PathBuf> = HashSet::new();

    queue.push_back(from.clone());
    visited.insert(from.clone());

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

fn print_neighborhood(seed: &PathBuf, depth: usize, levels: &[(Vec<PathBuf>, Vec<PathBuf>)]) {
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
            .get(0)
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
    let names: Vec<String> = path.iter().map(display_note_name).collect();
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

fn display_note_name(path: &PathBuf) -> String {
    path.file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned()
}

fn join_note_names(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .map(display_note_name)
        .collect::<Vec<_>>()
        .join("  ")
}

fn should_skip_entry(path: &Path, excludes: &[String]) -> bool {
    path.components().any(|component| {
        let os = component.as_os_str();
        os == OsStr::new(".obsidian")
            || os == OsStr::new(".git")
            || os == OsStr::new(".trash")
            || excludes.iter().any(|exclude| os == OsStr::new(exclude))
    })
}

fn collect_markdown_files(vault_root: &Path, excludes: &[String]) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = WalkDir::new(vault_root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| !should_skip_entry(entry.path(), excludes))
        .filter_map(|e| e.ok())
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("md"))
                .unwrap_or(false)
        })
        .map(|entry| entry.into_path())
        .collect();

    files.sort();
    files
}

fn collect_all_files(vault_root: &Path, excludes: &[String]) -> HashSet<UniCase<String>> {
    let mut known_assets: HashSet<UniCase<String>> = HashSet::new();

    for entry in WalkDir::new(vault_root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| !should_skip_entry(entry.path(), excludes))
        .filter_map(|e| e.ok())
        .filter(|entry| entry.file_type().is_file())
    {
        let path = entry.path();
        if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
            known_assets.insert(UniCase::new(file_name.to_owned()));
        }
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            known_assets.insert(UniCase::new(stem.to_owned()));
        }
    }

    known_assets
}

fn build_file_index(files: &[PathBuf]) -> HashMap<UniCase<String>, PathBuf> {
    let mut index: HashMap<UniCase<String>, PathBuf> = HashMap::new();

    for path in files {
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            let key = UniCase::new(stem.to_owned());
            if let Some(existing) = index.get(&key) {
                eprintln!(
                    "warning: duplicate stem {:?} — {:?} shadows {:?}; links to [[{}]] will resolve to the first path only",
                    stem, existing, path, stem
                );
            } else {
                index.insert(key, path.clone());
            }
        }
    }

    index
}

fn normalize_target(raw_target: &str) -> Option<String> {
    let trimmed = raw_target.trim();
    if trimmed.is_empty() {
        return None;
    }

    let after_hash = trimmed
        .split_once('#')
        .map(|(head, _)| head)
        .unwrap_or(trimmed);

    let no_anchor = after_hash
        .split_once('^')
        .map(|(head, _)| head)
        .unwrap_or(after_hash)
        .trim();

    if no_anchor.is_empty() {
        return None;
    }

    let last_component = no_anchor
        .rsplit('/')
        .next()
        .unwrap_or(no_anchor)
        .trim()
        .to_string();

    if last_component.is_empty() {
        None
    } else {
        Some(last_component)
    }
}

fn extract_wikilinks(content: &str) -> (Vec<String>, Vec<String>) {
    let stripped = strip_code_regions(content);
    let stripped = strip_html_comments(&stripped);
    let mut links = Vec::new();
    let mut embeds = Vec::new();

    for caps in wikilink_regex().captures_iter(&stripped) {
        let marker = caps.get(1).map(|m| m.as_str()).unwrap_or("");
        let target = caps.get(2).map(|m| m.as_str()).and_then(normalize_target);

        if let Some(target) = target {
            if marker == "!" {
                embeds.push(target);
            } else {
                links.push(target);
            }
        }
    }

    (links, embeds)
}

fn fenced_code_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?s)```.*?```").expect("valid fenced-code regex"))
}

fn inline_code_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"`[^`\n]*`").expect("valid inline-code regex"))
}

fn html_comment_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?s)<!--.*?-->").expect("valid html comment regex"))
}

fn wikilink_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(!?)\[\[([^\]|][^\]]*?)(?:\|([^\]]*))?\]\]").expect("valid wikilink regex")
    })
}

fn replace_range_with_spaces(buf: &mut [u8], start: usize, end: usize) {
    for byte in buf.iter_mut().take(end).skip(start) {
        *byte = b' ';
    }
}

fn strip_code_regions(text: &str) -> String {
    let mut bytes = text.as_bytes().to_vec();

    for mat in fenced_code_regex().find_iter(text) {
        replace_range_with_spaces(&mut bytes, mat.start(), mat.end());
    }

    let after_fenced = String::from_utf8(bytes).expect("valid UTF-8 after fenced replacement");
    let mut bytes = after_fenced.as_bytes().to_vec();
    for mat in inline_code_regex().find_iter(&after_fenced) {
        replace_range_with_spaces(&mut bytes, mat.start(), mat.end());
    }

    String::from_utf8(bytes).expect("valid UTF-8 after inline replacement")
}

fn strip_html_comments(text: &str) -> String {
    let mut bytes = text.as_bytes().to_vec();

    for mat in html_comment_regex().find_iter(text) {
        replace_range_with_spaces(&mut bytes, mat.start(), mat.end());
    }

    String::from_utf8(bytes).expect("valid UTF-8 after html comment replacement")
}

fn relativize(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

fn pluralize<'a>(count: usize, singular: &'a str, plural: &'a str) -> &'a str {
    if count == 1 {
        singular
    } else {
        plural
    }
}

fn absolute_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(path)
        }
    })
}

fn validate_vault_path(path: &Path) -> Result<PathBuf, String> {
    if !path.exists() {
        return Err(format!("vault path does not exist: {}", path.display()));
    }
    if !path.is_dir() {
        return Err(format!("vault path is not a directory: {}", path.display()));
    }

    fs::read_dir(path)
        .map_err(|err| format!("cannot read vault path {}: {err}", path.display()))?;

    Ok(absolute_path(path))
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
