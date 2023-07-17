import { danger, warn, fail, message } from 'danger';

const anyChange = (path) => {
  const changes = danger.git.modified_files.concat(danger.git.created_files).concat(danger.git.deleted_files);
  return changes.includes(path);
}

/* ------------ MRs need a description text ------------ */
if (danger.gitlab.mr.description.length < 10) {
  warn('Please include a description of your PR changes.');
}

/* ------------ Python Poetry dependency checks ------------ */
// Paths must end in slashes (except for root dir)
const pythonProjectDirs = [
  "",
  "examples/housing_demo/",
]

// Readable names for the project directory
const projectDirMarkdown = (dir) => dir || "mlopskit";

for (const projectDir of pythonProjectDirs) {
  const lockfileChanged = anyChange(`${projectDir}poetry.lock`);
  const pyprojectChanged = anyChange(`${projectDir}pyproject.toml`);

  // pyproject.toml and poetry.lock should probably be modified together
  if (pyprojectChanged && !lockfileChanged) {
    warn(
      `Python project \`${projectDirMarkdown(projectDir)}\`: \`pyproject.toml\` ` +
      "modified, but `poetry.lock` is unchanged. Did you forget to update the lockfile?"
    )
  }

  // Changes to poetry.lock require a rationale
  // Regex matches line-by-line, case-independent: https://regex101.com/r/SJj00W/1
  if (lockfileChanged && danger.gitlab.mr.description.search(/^# dependencies/gmi) === -1) {
    fail(
      `Python project \`${projectDirMarkdown(projectDir)}\`: Your MR includes a dependency change but no rationale. ` +
      "Please add a section '# Dependencies' to your MR description."
    )
  }
}
