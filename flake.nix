{
  description = "Env generation with secrets";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    secrets.url = "git+ssh://git@github.com/DPS25/nix.git";
  };

  outputs = { self, nixpkgs, secrets, ... }: let
    system = "x86_64-linux";
    pkgs = nixpkgs.legacyPackages.${system};
  in {
    devShells.${system}.default = pkgs.mkShell {
      name = "env-with-secrets";
      buildInputs = [ pkgs.sops pkgs.yq ];
        shellHook = ''
          echo "🔐 Loading secrets from ${secrets}/secrets"
          export SECRETS_DIR=${secrets}/secrets

          if [ -f "$SECRETS_DIR/main_influx.enc.yaml" ]; then
            echo "🔑 Loading main_influx.enc.yaml..."
            eval $(sops -d "$SECRETS_DIR/main_influx.enc.yaml" | \
              yq -r '. | to_entries[] | "export MAIN_\(.key | sub("INFLUX_ADMIN_TOKEN"; "INFLUX_TOKEN"))=\(.value)"')
          fi

          if [ -f "$SECRETS_DIR/sut_influx.enc.yaml" ]; then
            echo "🔑 Loading sut_influx.enc.yaml..."
            eval $(sops -d "$SECRETS_DIR/sut_influx.enc.yaml" | \
              yq -r '. | to_entries[] | "export SUT_\(.key | sub("INFLUX_ADMIN_TOKEN"; "INFLUX_TOKEN"))=\(.value)"')
          fi
            if [ -z "$ENV_NAME" ]; then
              echo "⚠️ \`ENV_NAME\` is not set"
            else
              ENV_FILE="./envs/$ENV_NAME.env"
              if [ -f "$ENV_FILE" ]; then
                echo "🔗 Creating symlink .env → $ENV_FILE"
                ln -sf "$ENV_FILE" .env

                # Export all variables from the env file to child processes and strip CRLFs.
                set -a
                . <(tr -d '\r' < "$ENV_FILE")
                set +a
              else
                echo "⚠️ \`$ENV_FILE\` not found"
                echo "please create a file at \`$ENV_FILE\` with your custom environment variables."
                echo "You can base it on \`./envs/example.env\`."
                echo "export ENV_NAME=YOURNAME && nix develop"
              fi
            fi
            source .venv/bin/activate
        '';

    };
  };
}
