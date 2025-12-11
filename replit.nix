{ pkgs }: {
  deps = [
    # Python runtime (handled by Replit's Python module)
    # System dependencies for Playwright browsers
    
    # Core browser dependencies (mapped from packages.txt)
    pkgs.nss                    # libnss3
    pkgs.nspr                   # libnspr4
    pkgs.atk                    # libatk1.0-0
    pkgs.at-spi2-atk            # libatk-bridge2.0-0, libatspi2.0-0
    pkgs.cups                   # libcups2
    pkgs.libdrm                 # libdrm2
    pkgs.gtk3                   # libgtk-3-0
    pkgs.gdk-pixbuf             # libgdk-pixbuf-2.0-0
    pkgs.pango                  # libpango-1.0-0, libpangocairo-1.0-0
    pkgs.cairo                  # libcairo2, libcairo-gobject2
    pkgs.alsa-lib               # libasound2
    pkgs.wayland                # libwayland-client0
    pkgs.libxkbcommon           # libxkbcommon0
    pkgs.xorg.libX11            # libx11-xcb1 (includes XCB)
    pkgs.xorg.libXcomposite     # libxcomposite1
    pkgs.xorg.libXdamage        # libxdamage1
    pkgs.xorg.libXext
    pkgs.xorg.libXfixes         # libxfixes3
    pkgs.xorg.libXrandr         # libxrandr2
    pkgs.xorg.libXrender
    pkgs.xorg.libXScrnSaver     # libxss1
    pkgs.xorg.libxshmfence      # libxshmfence1
    pkgs.xorg.libXcursor        # libxcursor1
    pkgs.xorg.libXtst           # libxtst6
    pkgs.mesa                   # libgbm1, libglu1-mesa (includes GBM and GLU)
    pkgs.glib
    
    # Additional libraries for Chromium
    pkgs.expat
    pkgs.fontconfig
    pkgs.freetype
    pkgs.zlib
    
    # XCB libraries (for X11)
    pkgs.xorg.libxcb
    pkgs.xorg.libXau
    pkgs.xorg.libXdmcp
    
    # Development tools (optional, but useful)
    pkgs.python311
  ];
}
