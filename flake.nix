{
  description = "Env generation with secrets";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";

    secrets = {
      url = "git+ssh://git@github.com/DPS25/nix.git";
    };
  };

  outputs = { self, nixpkgs, secrets, ... }: let
    system = "x86_64-linux";
    pkgs = nixpkgs.legacyPackages.${system};
  in {
    devShells.${system}.default = pkgs.mkShell {
      name = "env-with-secrets";
      buildInputs = [ pkgs.sops pkgs.yq pkgs.uv];

      shellHook = ''
        echo "🔐 Loading secrets from ${secrets}/secrets"
        uv sync
        export SECRETS_DIR=${secrets}/secrets

        if [ -f "$SECRETS_DIR/sut_influx.enc.yaml" ]; then
          echo "🔑 Loading sut_influx.enc.yaml..."
          eval $(sops -d "$SECRETS_DIR/sut_influx.enc.yaml" | \
            yq -r '. | to_entries | .[] | "export SUT_\(.key)=\(.value)"')
        fi
        if [ -f "$SECRETS_DIR/main_influx.enc.yaml" ]; then
          echo "🔑 Loading main_influx.enc.yaml..."
          eval $(sops -d "$SECRETS_DIR/main_influx.enc.yaml" | \
            yq -r '. | to_entries | .[] | "export MAIN_\(.key)=\(.value)"')
        fi
      '';
    };

  };
}