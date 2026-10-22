{
  description = "geo-line-ranker development shell";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
  # Node-only security backport; keep the existing Rust toolchain input.
  inputs.nodejs-nixpkgs.url = "github:NixOS/nixpkgs/04b65f5f819c8f0d0592a4b34fb3ee57ac92b1ad";

  outputs = { self, nixpkgs, nodejs-nixpkgs }:
    let
      nodejsFor = system:
        let
          pkgs = nodejs-nixpkgs.legacyPackages.${system};
        in
        assert pkgs.lib.versionAtLeast pkgs.nodejs_22.version "22.23.4";
        assert pkgs.lib.versionAtLeast pkgs.brotli.version "1.2.1";
        pkgs.nodejs_22;
      supportedSystems = [
        "aarch64-darwin"
        "x86_64-darwin"
        "aarch64-linux"
        "x86_64-linux"
      ];
    in
    {
      packages = nixpkgs.lib.genAttrs supportedSystems (system: {
        nodejs = nodejsFor system;
      });
      checks = nixpkgs.lib.genAttrs supportedSystems (system:
        let
          pkgs = nodejs-nixpkgs.legacyPackages.${system};
          nodejs = nodejsFor system;
        in
        {
          nodejs-security = pkgs.runCommand "nodejs-security" {
            nativeBuildInputs = [ nodejs ];
          } ''
            node <<'JS'
            const assert = require("node:assert/strict");
            const zlib = require("node:zlib");
            assert.equal(process.versions.node, "${nodejs.version}");
            assert.equal(process.versions.brotli, "${pkgs.brotli.version}");
            const input = Buffer.from("Brotli runtime check");
            assert.deepEqual(zlib.brotliDecompressSync(zlib.brotliCompressSync(input)), input);
            console.log(process.versions.node, process.versions.brotli);
            JS
            touch "$out"
          '';
        });
      devShells = nixpkgs.lib.genAttrs supportedSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShell {
            buildInputs = with pkgs; [
              cargo
              clippy
              just
              (nodejsFor system)
              pkg-config
              rustc
              rustfmt
            ];
          };
        });
    };
}
