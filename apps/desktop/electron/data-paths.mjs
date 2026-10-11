// data-paths.mjs — the pure path-resolution core, shared by the desktop app
// (via data-paths.ts, a typed re-export) and the CI smoke driver (which runs
// under Node's type-stripping and therefore cannot import the app's
// extensionless TypeScript directly). No Electron imports here; only node:path.
//
// data-paths.ts re-exports these names and adds the TypeScript-facing
// `HermesHomeOptions` interface. Keep the two in lockstep: every behavior in
// this file is exercised by data-paths.test.ts through the re-export.

import path from 'node:path'

/** A HERMES_HOME rooted inside a `profiles/` directory names the profile's
 * parent (the home), not the profile directory itself. */
function normalizeHermesHomeRoot(hermesHome, pathModule) {
  if (!hermesHome) {
    return hermesHome
  }
  const resolved = pathModule.resolve(String(hermesHome))
  const parent = pathModule.dirname(resolved)
  if (pathModule.basename(parent).toLowerCase() === 'profiles') {
    return pathModule.dirname(parent)
  }
  return resolved
}

/**
 * The LEGACY Hermes default home. This name is deliberately NOT Avunel's
 * default: `controlSocketPath` (ssh-connection.ts) derives the SSH
 * `desktop-ssh` directory from it, and that directory is a cross-process
 * contract pinned to `~/.hermes/desktop-ssh` (hermes_cli/main_dashboard.py,
 * remote-lifecycle.ts). Moving it would break desktop SSH. It stays put; the
 * Avunel default lives in `avunelPlatformDefaultHome` below.
 */
export function platformDefaultHermesHome(home, env = process.env, platform = process.platform) {
  const suffix = env.HERMES_DATA_DIR_SUFFIX || ''
  if (platform === 'win32') {
    const base = (env.LOCALAPPDATA || '').trim() || path.win32.join(home, 'AppData', 'Local')
    return path.win32.join(base, 'hermes') + suffix
  }
  return path.posix.join(home, '.hermes') + suffix
}

/** Avunel's own native default home (the data-home default for new installs). */
function avunelPlatformDefaultHome(home, env = process.env, platform = process.platform) {
  const suffix = env.HERMES_DATA_DIR_SUFFIX || ''
  if (platform === 'win32') {
    const base = (env.LOCALAPPDATA || '').trim() || path.win32.join(home, 'AppData', 'Local')
    return path.win32.join(base, 'avunel') + suffix
  }
  return path.posix.join(home, '.avunel') + suffix
}

export function resolveDesktopUserData(defaultPath, env = process.env) {
  return env.HERMES_DESKTOP_USER_DATA_DIR
    ? path.resolve(env.HERMES_DESKTOP_USER_DATA_DIR)
    : defaultPath + (env.HERMES_DATA_DIR_SUFFIX || '')
}

export function resolveDesktopHermesHome({ home, env = process.env, platform = process.platform, directoryExists = () => false, readWindowsHome = () => null }) {
  const paths = platform === 'win32' ? path.win32 : path.posix
  // AVUNEL_HOME is canonical; HERMES_HOME stays a legacy override.
  if (env.AVUNEL_HOME) {
    return normalizeHermesHomeRoot(env.AVUNEL_HOME, paths)
  }
  if (env.HERMES_HOME) {
    return normalizeHermesHomeRoot(env.HERMES_HOME, paths)
  }
  // Fresh-install rehearsals must not touch the real Hermes home.
  if (env.HERMES_DESKTOP_USER_DATA_DIR) {
    return paths.join(paths.resolve(env.HERMES_DESKTOP_USER_DATA_DIR), 'hermes-home')
  }
  if (platform === 'win32' && env.HERMES_HOME === undefined) {
    // Explorer can miss setx changes. An explicit empty value opts out of that fallback.
    const registryHome = readWindowsHome()
    if (registryHome) {
      return normalizeHermesHomeRoot(registryHome, paths)
    }
  }
  const avunelHome = avunelPlatformDefaultHome(home, env, platform)
  const legacyHome = platformDefaultHermesHome(home, env, platform)
  // Reuse an existing legacy ~/.hermes install so its data is never orphaned
  // or moved without an explicit override. Skipped for isolated suffix runs.
  if (!env.HERMES_DATA_DIR_SUFFIX && !directoryExists(avunelHome) && directoryExists(legacyHome)) {
    return legacyHome
  }
  return avunelHome
}
