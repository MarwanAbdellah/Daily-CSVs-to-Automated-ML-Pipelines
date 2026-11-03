# docs/

Documentation assets for the root [README](../README.md).

## `diagrams/`

The hand-drawn diagrams were made with [Sketchy](https://github.com/ddkbs/sketchy) (v0.1.6, MIT). Each diagram has two files:

| Source (`.sketchy`) | Rendered (`.svg`) | Shows |
|---|---|---|
| `architecture.sketchy` | `architecture.svg` | Both DAGs inside Docker Compose, the Postgres load and the `gold_orders` Asset trigger |
| `data-quality.sketchy` | `data-quality.svg` | Which problems Silver fixes with rules and which steps the training pipeline learns |
| `ml-pipeline.sketchy` | `ml-pipeline.svg` | The time-based split and the leakage-safe sklearn Pipeline |

A `.sketchy` file is Sketchy's scene syntax. Each line declares a node, group or arrow; Sketchy computes the layout. The SVGs embed their fonts and an opaque background, so they read the same in GitHub's light and dark themes.

### Re-rendering a diagram

1. Download `sketchy-skill-<version>.zip` from the [Sketchy releases](https://github.com/ddkbs/sketchy/releases) and unzip it.
2. Render the scene to a standalone HTML page:

   ```bash
   docker run --rm -v "<unzipped>/sketchy/scripts:/sketchy:ro" -v "$PWD/docs/diagrams:/work" -w /work node:24-slim node /sketchy/render.mjs architecture.sketchy architecture.html
   ```

   On macOS or Linux you can run `node <unzipped>/sketchy/scripts/render.mjs …` directly. The Docker form avoids a Windows path bug in v0.1.6.
3. Open the HTML page in a browser and use its **SVG** export button.
