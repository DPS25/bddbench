{
  description = "Env generation with secrets";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    secrets.url = "git+ssh://git@github.com/DPS25/nix.git";
  };

  outputs = { self, nixpkgs, secrets, ... }: let
    system = "x86_64-linux";
    pkgs = nixpkgs.legacyPackages.${system};
      libPath = nixpkgs.lib.makeLibraryPath [
    pkgs.systemd.dev
    pkgs.gcc
    pkgs.stdenv.cc.cc.lib
    pkgs.zlib
  ];
  in {
    devShells.${system}.default = pkgs.mkShell {
      name = "env-with-secrets";
      buildInputs = [ pkgs.sops pkgs.yq pkgs.uv pkgs.python314FreeThreading pkgs.pkg-config pkgs.systemd.dev pkgs.gcc pkgs.stdenv.cc.cc.lib pkgs.zlib];


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
