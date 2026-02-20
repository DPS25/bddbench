{
  description = "Env generation with secrets";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    secrets.url = "git+ssh://git@github.com/DPS25/nix.git";
  };

  outputs =
    {
      self,
      nixpkgs,
      secrets,
      ...
    }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
      libPath = nixpkgs.lib.makeLibraryPath [
        pkgs.systemd.dev
        pkgs.gcc
        pkgs.stdenv.cc.cc.lib
        pkgs.zlib
      ];
    in
    {
      formatter.${system} = pkgs.nixfmt-tree;

      devShells.${system}.default = pkgs.mkShell {
        name = "env-with-secrets";
        buildInputs = [
          pkgs.sops
          pkgs.yq
          pkgs.uv
          pkgs.python314FreeThreading
          pkgs.pkg-config
          pkgs.systemd.dev
          pkgs.gcc
          pkgs.stdenv.cc.cc.lib
          pkgs.zlib
          pkgs.sysbench
          pkgs.fio
          pkgs.mutagen

          (pkgs.writeShellScriptBin "run-behave-normal" ''
            set -e
            behave -t="write and normal and singlebucket"  -f progress3 --no-skipped --no-snippets --no-summary
            behave -t="write and normal and multibucket"  -f progress3 --no-skipped --no-snippets --no-summary
            behave -t="query and normal"  -f progress3 --no-skipped --no-snippets --no-summary
            behave -t="delete" -f progress3 --no-skipped --no-snippets --no-summary
          '')

          (pkgs.writeShellScriptBin "run-behave-normal-5-times" ''
            set -e
            for i in {1..10}; do run-behave-normal; sleep 1; done
          '')

          # ... inside buildInputs ...

          (pkgs.writeShellScriptBin "run-full-benchmark-suite" ''
            set -e
            mkdir -p reports/plots

            # NEW: Record the global start time for this version's run
            date -u +"%Y-%m-%dT%H:%M:%SZ" >> .suite_start_times

            export PYTHONPATH=.
            run_block() {
              local tag="$1"; local feature="$2"; local measurement="$3"
              local start_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
              for i in {1..5}; do
                behave -t="$tag" -f progress3 --no-skipped --no-snippets --no-summary
              done
              local end_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
              python src/evaluation/plot_results.py --start "$start_time" --end "$end_time" \
                --measurement "$measurement" --feature "reports/plots/$feature"
            }

            run_block "write and normal and singlebucket" "write_single" "bddbench_write_result"
            run_block "write and normal and multibucket" "write_multi" "bddbench_multi_write_result"
            run_block "query and normal" "query_perf" "bddbench_query_result"

            # NEW: Record the global end time
            date -u +"%Y-%m-%dT%H:%M:%SZ" >> .suite_end_times
          '')

          (pkgs.writeShellScriptBin "run-comparison-report" ''
            set -e
            if [ ! -f .suite_start_times ]; then
              echo "❌ No timestamp logs found. Run benchmarks first or check .suite_start_times"
              exit 1
            fi

            GLOBAL_START=$(sort .suite_start_times | head -n 1)
            GLOBAL_END=$(sort .suite_end_times | tail -n 1)

            mkdir -p reports/plots/comparisons

            # Format: measurement:field_name:file_label
            # Note: query results use 'total_avg_s', not 'latency_avg_s'
            KPI_LIST=(
              "bddbench_write_result:throughput_points_per_s:write_throughput"
              "bddbench_query_result:total_avg_s:query_latency"
              "bddbench_delete_result:total_duration_s:delete_performance"
            )

            for entry in "''${KPI_LIST[@]}"; do
              IFS=":" read -r measurement kpi label <<< "$entry"
              echo "📊 Generating Comparison for $label ($kpi)..."
              python src/evaluation/plot_comparison.py \
                --start "$GLOBAL_START" \
                --end "$GLOBAL_END" \
                --measurement "$measurement" \
                --kpi "$kpi" \
                --feature "reports/plots/comparisons/$label"
            done
          '')

          (pkgs.writeShellScriptBin "run-everything-5-times" ''
            set -e
            run-behave-host-benchmarks-5-times
            run-behave-normal-5-times
          '')

          (pkgs.writeShellScriptBin "run-behave-host-benchmarks-5-times" ''
            set -e
            for i in {1..5}; do behave -t="memory" -f progress3 --no-skipped --no-snippets --no-summary; sleep 1; done
            for i in {1..5}; do behave -t="storage" -f progress3 --no-skipped --no-snippets --no-summary; sleep 1; done
            for i in {1..5}; do behave -t="cpu" -f progress3 --no-skipped --no-snippets --no-summary; sleep 1; done
          '')

        ];

        env = {
          NIX_LD_LIBRARY_PATH = libPath;
          LD_LIBRARY_PATH = libPath;
        };

        shellHook = ''

          echo "🔐 Loading secrets from ${secrets}/secrets"
          export SECRETS_DIR=${secrets}/secrets

          # Force uv t use python provided by Nix (avoid ~/.local/share/uv/... on NixOS)
          export UV_PYTHON="${pkgs.python314FreeThreading}/bin/python3"
          export UV_PYTHON_DOWNLOADS=never
          export UV_PROJECT_ENVIRONMENT=".venv"

          uv sync

          # =====================================
          # 1. Start fresh merged .env
          # =====================================
          rm -f .env
          touch .env

          # =====================================
          # 2. Load user env first
          # =====================================
          if [ -z "$ENV_NAME" ]; then
            echo "⚠️ \`ENV_NAME\` is not set"
          else
            ENV_FILE="./envs/$ENV_NAME.env"
            if [ -f "$ENV_FILE" ]; then
              echo "📄 Loading user env: $ENV_FILE"
              tr -d '\r' < "$ENV_FILE" >> .env
              echo "" >> .env
            else
              echo "⚠️ \`$ENV_FILE\` not found"
              echo "Create it based on ./envs/example.env"
              echo "export ENV_NAME=YOURNAME && nix develop"
            fi
          fi


          # =====================================
          # 3. Load and merge secrets
          #    MAIN → INFLUXDB_MAIN_TOKEN
          #    SUT  → INFLUXDB_SUT_TOKEN
          # =====================================
          load_secret_file () {
            local file="$1"
            local envprefix="$2"

            if [ -f "$file" ]; then
              echo "🔑 Merging secrets from $(basename "$file")"
              sops -d "$file" | \
                yq -r --arg envprefix "$envprefix" '
                  to_entries[] |
                    # rename INFLUX_ADMIN_TOKEN → INFLUX_TOKEN
                    .key |= sub("INFLUX_ADMIN_TOKEN"; "INFLUX_TOKEN") |
                    # final rename: INFLUX_* → <envprefix>*
                    "\($envprefix)\(.key | sub("^INFLUX_"; ""))=\(.value)"
                ' >> .env
            fi
          }

          load_secret_file "$SECRETS_DIR/main_influx.enc.yaml" "INFLUXDB_MAIN_"
          load_secret_file "$SECRETS_DIR/sut_influx.enc.yaml"  "INFLUXDB_SUT_"


          # =====================================
          # 4. Export merged .env into the shell
          # =====================================
          echo "📤 Exporting merged .env"
          set -a
          . <(tr -d '\r' < .env)
          set +a

          # Re-apply user env overrides (so local overrides win over secrets)
          if [ -n "$ENV_NAME" ]; then
            ENV_FILE="./envs/$ENV_NAME.env"
            if [ -f "$ENV_FILE" ]; then
              echo "📄 Re-applying user env overrides into shell: $ENV_FILE"
              set -a
              . <(tr -d '\r' < "$ENV_FILE")
              set +a
            fi
          fi

          # =====================================
          # 5. Activate Python venv
          # =====================================
          echo "🐍 Activating virtual environment..."
          if [ -f .venv/bin/activate ]; then
            echo "✅ .venv found."
            source ./.venv/bin/activate
            echo "✅ .venv activated."
          else
            echo "❌ .venv was not created (uv sync failed)."
            exit 1
          fi
          echo "done."
        '';

      };
    };
}
