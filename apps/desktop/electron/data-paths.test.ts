import assert from 'node:assert/strict'
import os from 'node:os'
import path from 'node:path'

import { afterEach, test, vi } from 'vitest'

import { platformDefaultHermesHome, resolveDesktopHermesHome, resolveDesktopUserData } from './data-paths'
import { controlSocketPath } from './ssh-connection'

afterEach((): void => {
  vi.unstubAllEnvs()
})

test.skipIf(process.platform === 'win32')('local SSH sockets use the suffixed default root', (): void => {
  vi.stubEnv('HERMES_DATA_DIR_SUFFIX', 'magic-test')
  const socket: string = controlSocketPath('user', 'host', 22)

  assert.equal(path.dirname(socket), path.join(platformDefaultHermesHome(os.homedir()), 'desktop-ssh'))
})

test('default data roots append the suffix literally on each platform', (): void => {
  for (const platform of ['linux', 'darwin', 'win32'] as const) {
    const paths: typeof path = platform === 'win32' ? path.win32 : path.posix
    const home: string = platform === 'win32' ? 'C:\\Users\\test' : '/home/test'
    const local: string = paths.join(home, 'AppData', 'Local')
    const userData: string = paths.join(home, 'app-data', 'Hermes')
    // platformDefaultHermesHome stays the LEGACY ~/.hermes root (the SSH
    // desktop-ssh contract depends on it); resolveDesktopHermesHome defaults
    // to Avunel's own ~/.avunel.
    const legacyBase: string = platform === 'win32' ? paths.join(local, 'hermes') : paths.join(home, '.hermes')
    const avunelBase: string = platform === 'win32' ? paths.join(local, 'avunel') : paths.join(home, '.avunel')

    for (const suffix of ['', '-asdfasdf', 'magic-test', ' spaced ']) {
      const env: NodeJS.ProcessEnv = { LOCALAPPDATA: local, HERMES_DATA_DIR_SUFFIX: suffix }

      assert.equal(platformDefaultHermesHome(home, env, platform), legacyBase + suffix)
      assert.equal(resolveDesktopUserData(userData, env), userData + suffix)
      assert.equal(
        resolveDesktopHermesHome({ home, env, platform, directoryExists: (): boolean => false }),
        avunelBase + suffix
      )
    }
  }
})

test('explicit homes and userData retain precedence, and suffixed Windows homes never use legacy state', (): void => {
  const home: string = '/home/test'

  const env: NodeJS.ProcessEnv = {
    HERMES_DATA_DIR_SUFFIX: 'magic-test',
    HERMES_HOME: '/explicit/home',
    HERMES_DESKTOP_USER_DATA_DIR: '/explicit/electron'
  }

  assert.equal(resolveDesktopUserData('/default/electron', env), path.resolve(env.HERMES_DESKTOP_USER_DATA_DIR!))
  assert.equal(resolveDesktopHermesHome({ home, env, platform: 'linux' }), env.HERMES_HOME)
  delete env.HERMES_HOME
  assert.equal(resolveDesktopHermesHome({ home, env, platform: 'linux' }), '/explicit/electron/hermes-home')

  const windowsHome: string = 'C:\\Users\\test'
  const windowsEnv: NodeJS.ProcessEnv = { HERMES_DATA_DIR_SUFFIX: 'magic-test' }
  const expected: string = path.win32.join(windowsHome, 'AppData', 'Local', 'avunelmagic-test')

  assert.equal(
    resolveDesktopHermesHome({
      home: windowsHome,
      env: windowsEnv,
      platform: 'win32',
      directoryExists: (): boolean => true
    }),
    expected
  )
  assert.equal(
    resolveDesktopHermesHome({
      home: windowsHome,
      env: windowsEnv,
      platform: 'win32',
      readWindowsHome: (): string => 'C:\\custom'
    }),
    'C:\\custom'
  )
})

test('AVUNEL_HOME is canonical and a lone legacy ~/.hermes install is reused', (): void => {
  const home: string = '/home/test'

  assert.equal(
    resolveDesktopHermesHome({ home, env: { AVUNEL_HOME: '/canonical/avunel', HERMES_HOME: '/legacy/hermes' }, platform: 'linux' }),
    '/canonical/avunel'
  )
  // No Avunel home on disk but a legacy one -> reuse the legacy install.
  assert.equal(
    resolveDesktopHermesHome({
      home,
      env: {},
      platform: 'linux',
      directoryExists: (p: string): boolean => p === '/home/test/.hermes'
    }),
    '/home/test/.hermes'
  )
  // Brand-new machine -> Avunel's own default.
  assert.equal(
    resolveDesktopHermesHome({ home, env: {}, platform: 'linux', directoryExists: (): boolean => false }),
    '/home/test/.avunel'
  )
})
