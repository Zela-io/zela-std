{
	pkgs ? import <nixpkgs> {}
}: let
	# TODO: this obviously only works for aarch64 macos
	wasi_sdk = builtins.fetchTarball {
		url = "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-27/wasi-sdk-27.0-arm64-macos.tar.gz";
		sha256 = "sha256:06i3x95airmk5gs00831g1xmbpq5sard3g18rkzxzm9ikf34s4i7";
	};
in

pkgs.mkShellNoCC {
	packages = with pkgs; [
		just
		rustup wasm-tools
		openssl pkg-config

		llvmPackages_20.clang
	];

	# these are to build procedures on macos
	CC_wasm32_wasip2 = "${wasi_sdk}/bin/clang";
	WASI_SYSROOT = "${wasi_sdk}/share/wasi-sysroot";
}
