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


          (pkgs.writeShellScriptBin "run-full-benchmark-suite" ''
            set -e
            CUR_V=''${SUT_VERSION:-"unknown"}
            PLOT_DIR="reports/plots/versions/$CUR_V"
            mkdir -p "$PLOT_DIR"

            # Log start for comparison report
            date -u +"%Y-%m-%dT%H:%M:%SZ" >> .suite_start_times
            export PYTHONPATH=.

            run_block() {
              local tag="$1"; local feat="$2"; local meas="$3"
              echo "  -> Benchmarking $tag..."
              local b_start=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
              for i in {1..5}; do
                behave -t="$tag" -f progress3 --no-skipped --no-snippets --no-summary
                sleep 2s
              done
              local b_end=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
              python src/evaluation/plot_results.py --start "$b_start" --end "$b_end" \
                --measurement "$meas" --feature "$PLOT_DIR/$feat"
            }

            # Your Requested Suite
            run_block "write and normal and singlebucket" "write_single" "bddbench_write_result"
            run_block "write and normal and multibucket"  "write_multi"  "bddbench_multi_write_result"
            run_block "query and normal and singlebucket"  "query_single"   "bddbench_query_result"
            run_block "query and normal and multibucket"  "query_multi"  "bddbench_query_write_result"
            run_block "delete and not multibucket"                            "delete"  "bddbench_delete_result"
            run_block "multibucket and delete"            "delete_multi" "bddbench_multi_delete_result"
            run_block "me and normal"   "user_me"   "bddbench_user_benchmark_summary"
            run_block "crud and normal"  "user_crud" "bddbench_user_benchmark_summary"

            date -u +"%Y-%m-%dT%H:%M:%SZ" >> .suite_end_times
          '')

          (pkgs.writeShellScriptBin "run-all-matrix-versions" ''
            set -e
            # Ensure we use the secrets path provided by the flake input
            MATRIX=("2.1.1:benedikt-influx2-1-1" "2.4.0:benedikt-influx2-4-0" "2.5.1:benedikt-influx2-5-1" "2.7.6:benedikt-influx2-7-6")

            rm -f .suite_start_times .suite_end_times
            mkdir -p reports/plots/versions reports/plots/comparisons

            for entry in "''${MATRIX[@]}"; do
              IFS=":" read -r VERSION ATTR <<< "$entry"
              echo "🏗️  Deploying InfluxDB $VERSION via ${secrets}"

              ssh nixos@dsp25-benedikt "sudo rm -rf /var/lib/influxdb2/"

              nixos-rebuild switch --flake "${secrets}#$ATTR" \
                --target-host nixos@dsp25-benedikt --sudo
              sleep 2s
              SUT_VERSION=$VERSION run-full-benchmark-suite
            done
            run-comparison-report
          '')

(pkgs.writeShellScriptBin "run-comparison-report" ''
  set -e
  [ ! -f .suite_start_times ] && echo "❌ No timestamp logs found." && exit 1

  # Get the absolute start of the first test and end of the last test
  G_START=$(sort .suite_start_times | head -n 1)
  G_END=$(sort .suite_end_times | tail -n 1)

  echo "📅 Full Range: $G_START to $G_END"

  KPI_LIST=(
    "bddbench_write_result:throughput_points_per_s:write_throughput"
    "bddbench_multi_write_result:throughput_points_per_s:write_multi_throughput"
    "bddbench_query_result:total_avg_s:query_latency"
    "bddbench_delete_result:total_duration_s:delete_latency"
    "bddbench_multi_delete_result:total_duration_s:delete_multi_latency"
    "bddbench_user_benchmark_summary:latency_avg_ms:user_me_latency"
    "bddbench_user_benchmark_summary:throughput_ops_s:user_crud_throughput"
  )

  mkdir -p reports/plots/comparisons

  for entry in "''${KPI_LIST[@]}"; do
    IFS=":" read -r meas kpi label <<< "$entry"
    echo "📊 Generating multi-version comparison for $label..."
    python src/evaluation/plot_comparison.py --start "$G_START" --end "$G_END" \
      --measurement "$meas" --kpi "$kpi" --feature "reports/plots/comparisons/$label"
  done
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
