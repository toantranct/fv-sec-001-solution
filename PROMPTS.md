Prompt 1: Read /recruitment/fv-sec-001-software-engineer-challenge/README.md and build a complete TypeScript CLI solution in a new folder fv-sec-001-test-solution.

Stack: Node.js + TypeScript.

---

Goal:
- Stream-read CSV (DO NOT load full file)
- Validate rows (skip malformed safely)
- Aggregate by campaign_id:
  - impressions, clicks, cost, conversions
- Compute:
  - CTR = clicks / impressions
  - CPA = cost / conversions
- Output:
  - results/top10_ctr.csv (highest CTR)
  - results/top10_cpa.csv (lowest CPA)

---

Structure:
- src/cli.ts
- src/csv.ts
- src/aggregator.ts
- src/output.ts
- test/aggregator.test.ts
- package.json, tsconfig.json, README.md

---

Rules:
- Must use streaming
- No full in-memory sorting (no Array.from on large data)
- Handle invalid/malformed rows
- Clean, modular, typed code

---

Steps:
1. Read /recruitment/fv-sec-001-software-engineer-challenge/README.md
2. Inspect data/ad_data.csv (sample rows, detect schema)
3. Summarize schema + risks
4. Implement CLI, parser, aggregator, output
5. Add tests + update README


Prompt 2: 
Generate a real benchmark file `benchmark.txt` for this Node.js CLI.

Requirements:
- Run: node dist/src/cli.js --input data/ad_data.csv --output results
- Use `/usr/bin/time -v` to collect full metrics
- Redirect benchmark output into `benchmark.txt`
- Do NOT fake data — must be actual runtime results
- Ensure file contains fields like:
  - Command being timed
  - User/System time
  - CPU %
  - Elapsed time
  - Max memory
  - Exit status

Steps:
1. Build project if needed
2. Ensure `results/` exists
3. Run benchmark and save output to `benchmark.txt`

Output:
- Confirm file created
- Print the exact command used


Prompt 3: 
Using direct parsing with readline + split(",") is less abstract and requires less memory allocation. Reviewing benchmark history with the original approach, then using new approaches for time-based comparison. Reviewing benchmark history with the original approach, then with the new approaches for comparison.

Prompt 4: 
Run the comparison again, then update the comparison in README.md. There's no need to create a comparison.txt file; delete the unnecessary benchmark.txt file outside the project.

