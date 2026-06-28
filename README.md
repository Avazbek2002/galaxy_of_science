# Map of Science — 3D Citation Galaxy

> A planetary-scale graph visualization of human knowledge, built on 112 million academic papers and 2.5 billion citation edges.

![Map of Science](https://img.shields.io/badge/papers-112M-6b5bff?style=flat-square) ![Edges](https://img.shields.io/badge/citations-2.5B-b060ff?style=flat-square) ![Stack](https://img.shields.io/badge/stack-PySpark%20%7C%20Delta%20Lake%20%7C%20FastAPI%20%7C%20Three.js-48cae4?style=flat-square) ![Deploy](https://img.shields.io/badge/deploy-Vercel-black?style=flat-square)

**[→ Live Demo: science-galaxy.avazbek.xyz](https://science-galaxy.avazbek.xyz/)**
<img width="1512" height="855" alt="image" src="https://github.com/user-attachments/assets/25eb602c-5e32-46f8-8138-2b5f61a0dda9" />

---

## What It Is

Every academic paper ever published cites other papers. Those citations form a directed graph — a map of how ideas build on each other across every field of human inquiry. This project ingests the entire [OpenAlex](https://openalex.org/) dataset, runs distributed PageRank across the full graph, and renders the most influential papers as a living, interactive 3D galaxy in the browser.

Clustered by scientific field. Sized by influence. Colored by domain. Explorable in real time.

---

## The Scale Problem

The OpenAlex dataset lives on a public S3 bucket as thousands of compressed JSON files. Processing it naively is not feasible — the raw citation graph has:

| Metric | Count |
|---|---|
| Academic papers ingested | **112,369,197** |
| Citation edges mapped | **~2.5 billion** |
| Scientific fields discovered | **48+** |
| Domains | Physical Sciences, Life Sciences, Health Sciences, Social Sciences |

Getting this into a queryable, PageRank-ready state required a proper data engineering pipeline.

---

## Architecture

### 1. Ingestion Pipeline — `download.py`

Runs on **Databricks Serverless** with PySpark and streams the entire OpenAlex S3 bucket chunk by chunk.

**Key design decisions:**
- **Fault-tolerant checkpointing** — every processed file key is saved to a Delta Lake checkpoint table. If the job crashes or times out, it resumes from exactly where it left off. No data is reprocessed, no data is lost.
- **Atomic writes** — each compressed file is fully parsed in memory before anything is written to Delta Lake. A partial file never poisons the dataset.
- **Zero-citation filtering** — papers with no citations are dropped at ingestion time, cutting the graph size significantly while preserving all structurally meaningful nodes.
- **Schema enforcement** — explicit PySpark schemas with typed fields prevent silent type coercion across billions of rows.
- **Auto-compaction** — Delta Lake's `optimizeWrite` and `autoCompact` properties are enabled to keep file layouts efficient at scale.

The pipeline extracts each paper's numeric ID, title, citation count, scientific domain, and field classification from the raw OpenAlex JSON, then separately extracts every `referenced_works` pair as a directed edge.

### 2. Distributed PageRank — `pagerank.py`

Standard iterative PageRank does not scale to 2.5 billion edges without careful engineering. Naive Spark DAG chaining causes the lineage graph to grow unboundedly across iterations, eventually causing out-of-memory failures or query plan explosions.

**Solutions applied:**
- **DAG lineage breaking** — after each iteration, the new rank vector is materialized to a Delta Lake staging table and read back fresh. This resets Spark's query plan to a constant depth regardless of iteration count.
- **Alternating staging tables** (`step_a` / `step_b`) — prevents read/write conflicts caused by lazy evaluation on the same table within a single iteration.
- **Shuffle partition tuning** — set to 2,000 partitions to match the cardinality of a 2.5B edge join.
- **Early stopping** — convergence is measured as the L1 norm (sum of absolute rank deltas) across all nodes. Iteration stops as soon as the graph stabilizes below a 0.001 threshold, typically well before the 15-iteration cap.

**PageRank parameters:**
```
Damping factor:          0.85
Max iterations:          15
Convergence threshold:   0.001
Initial rank per node:   1 / 112,369,197
```

### 3. API Server — `server.py`

A **FastAPI** backend that connects to Databricks SQL and serves the graph data to the frontend.

- Queries the top **500 nodes per scientific field** by PageRank score from the pre-filtered top-0.1% table.
- Fetches only the **induced subgraph** — edges where both endpoints are in the capped node set, ensuring the frontend never receives orphaned references.
- Results are cached in-process with `@lru_cache` so the Databricks query runs once per server lifetime.
- Deployed to **Vercel** via `vercel.json`.

### 4. 3D Visualization — `map-of-science.html`

A self-contained **Three.js** WebGL application that turns the API response into a navigable 3D universe.

**Layout engine:**
- Field cluster centers are positioned using a **force-directed simulation** — fields with stronger cross-citation relationships are pulled closer together, weakly related fields repel to the periphery. 600 iterations with simulated annealing cooldown.
- Individual nodes are scattered in a spherical volume around their field's cluster center.
- Node radius scales with the **log-normalized PageRank score** — the most influential papers are visibly larger.

**Rendering:**
- **Instanced rendering** per color group — nodes sharing a field color are drawn in a single GPU draw call, keeping the renderer fast even with thousands of visible nodes.
- **Bezier-curved edges** with a custom GLSL shader — edges curve gently toward the origin, giving the galaxy its organic look. Thickness and opacity are proportional to the geometric mean of the connected nodes' PageRank scores.
- **Additive blending** on all edges creates the glowing nebula effect where citation paths overlap.
- **WebGL fog** adds depth perception across the 3D space.
- **3,000-star background** for spatial orientation.

**Interaction:**
- Auto-rotating camera with drag-to-orbit and scroll-to-zoom
- PageRank threshold slider to progressively reveal or hide lower-influence nodes
- Paper search by title or field
- Hover tooltip showing paper title, field, domain, and influence score
- Toggle edge visibility

---

## Stack

| Layer | Technology |
|---|---|
| Data ingestion | PySpark on Databricks Serverless |
| Storage | Delta Lake (S3-backed) |
| Graph computation | PySpark distributed PageRank |
| API | FastAPI + Databricks SQL Connector |
| Frontend | Three.js (WebGL), vanilla JS |
| Deployment | Vercel |

---

## Running Locally

**Prerequisites:** Python 3.10+, a Databricks workspace with the Delta tables populated.

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
cp .env.example .env
# Fill in DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH

# Start the server
uvicorn server:app --reload
```

Open `http://localhost:8000` — the HTML frontend is served directly by FastAPI.

---

## Data Source

All paper metadata and citation records come from [OpenAlex](https://openalex.org/), an open-access index of global research maintained by OurResearch. The dataset is publicly available on AWS S3 under an open license.

---

## What Makes This Hard

Running PageRank on a billion-edge graph is not a matter of writing the algorithm — it's a matter of managing state at scale without the system collapsing under its own weight. The engineering challenges here were:

1. **Resumable ingestion** at petabyte scale without reprocessing on failure
2. **Preventing Spark DAG explosion** across 15 iterative graph passes
3. **Fitting the result into a browser** — going from 112M nodes to a smooth, interactive render required aggressive filtering (top 0.1% by PageRank), smart field-level sampling, and GPU instancing
4. **Making the layout meaningful** — a random 3D scatter would be noise; the force-directed field clustering makes the disciplinary structure of science legible at a glance

---

*Built as a demonstration of large-scale distributed data engineering and real-time graph visualization.*
