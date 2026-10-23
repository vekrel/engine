Adapted from T3 Code's `docs/user/t3-connect-troubleshooting.md` ([pingdotgg/t3code](https://github.com/pingdotgg/t3code) at 8d3b061). MIT License, Copyright (c) 2026 T3 Tools Inc.; see [NOTICE](../NOTICE). Highly inspired by T3 Code.

# Vekrel Connect troubleshooting

A Vekrel Connect connection goes through four parts, in this order:

1. **This device** reaches Vekrel Connect over the internet.
2. **Vekrel Connect** checks your sign-in and the host's link.
3. **The host's tunnel** (cloudflared on the host) carries the request to the host.
4. **Vekrel on the host** answers.

When a connection fails, open **Settings > Connections**, find the machine,
and select **Details**. The first part marked as failed is the cause. Find its
section below.

To see the host's side, run `vekrel connect status` on the host, or open
**Settings > Connections** on the host. Both check the host's Vekrel Connect
address from the host, and say why it fails. `vekrel connect` runs the same check
when it finishes setup. This check cannot test another device's network. For a
problem on one device, use **Details** on that device.

## The host's tunnel is offline

Vekrel Connect could not reach the host's tunnel. This means one of these:

- The host is asleep, turned off, or offline.
- Vekrel is not running on the host.
- The host's network blocks the tunnel. See
  [Port 7844 is blocked on the host](#port-7844-is-blocked-on-the-host).

Fix:

1. Make sure the host is awake and Vekrel runs on it.
2. On the host, run `vekrel connect status`. It names the problem on the host's side.
3. If the host was offline for a long time, Vekrel Connect removes its tunnel. Start
   Vekrel on the host and update it to the latest version. It gets a new tunnel.

## Port 7844 is blocked on the host

The tunnel on the host connects out to Cloudflare on port 7844, with TCP and
UDP. Both tunnel protocols (QUIC and HTTP/2) use this port. When the host's
network blocks it, the tunnel cannot start, but normal web traffic still works.
The host looks online, so this problem is easy to miss.

Common causes:

- **A Tailscale exit node on the host.** All of the host's internet traffic goes
  through the exit node, and that network blocks port 7844. Turn off the exit
  node on the host with `tailscale set --exit-node=`, or allow outbound port
  7844 on the exit node's network.
- **A VPN on the host.** Work VPNs often block unknown ports. Ask for port 7844
  to be allowed, or use a direct route instead (see below).
- **A firewall or strict network.** Allow outbound TCP and UDP port 7844.

If you cannot unblock the port, you can connect without Vekrel Connect. When the
host and this device are on the same Tailscale network, add the host's
Tailscale address as a direct route. See
[Reach one machine several ways](./remote-access.md#reach-one-machine-several-ways).

## The tunnel works, but Vekrel does not answer

The host's tunnel is up, but it cannot reach Vekrel on the host. Common causes:

- Vekrel on the host stopped, crashed, or is still starting.
- Vekrel restarted on a different port, for example after an update, and the
  tunnel still points at the old port.
- The server binds only one network address with `VEKREL_HOST`. The tunnel
  sends requests to `127.0.0.1`, so the server must also listen there.

Fix: restart Vekrel on the host. For the background service, run
`vekrel service restart`. Then run `vekrel connect status` on the host.

## The Vekrel Connect address does not resolve or answer

The host's Vekrel Connect address did not resolve, refused the connection, failed a
secure (TLS) connection, or did not answer in time.

Fix:

1. Make sure the host is awake and online.
2. On the host, run `vekrel connect status`.
3. If the check fails only on the host, check the host's DNS, VPN, or proxy.

## This device can't reach Vekrel Connect

The problem is on this device's network, not on the host. This device could
not reach Vekrel Connect, or it reached Vekrel Connect but not the host's tunnel
address.

Fix:

1. Check this device's internet connection. On a captive or dead Wi-Fi network,
   the device can look online but cannot reach anything.
2. A VPN, firewall, ad blocker, or DNS filter on this device can block Vekrel
   Connect. Turn it off for a moment to test.
3. On iPhone, Connectivity Assist in the Wi-Fi settings can break Vekrel Connect.
   Turn it off to test.
4. Try another network, or add a direct route to the host, such as its
   Tailscale address. See
   [Reach one machine several ways](./remote-access.md#reach-one-machine-several-ways).

## Sign-in, account, or link problems

Vekrel Connect does not have an active link between your account and the host.

- **The host is linked to a different account.** Sign in to the same Vekrel Connect
  account on this device and on the host. On the host, run `vekrel connect logout`,
  then `vekrel connect`.
- **Vekrel Connect is off on the host, or the host only publishes agent activity.**
  On the host, open **Settings > Connections** and turn **Vekrel Connect** on, or
  run `vekrel connect`.
- **The host was linked, but the server did not apply the link yet.** Run
  `vekrel service restart` on the host, or restart `vekrel serve`.
- **This device's sign-in expired, or the host no longer accepts this
  device.** Sign out of Vekrel Connect and sign in again.
- **Your account reached its tunnel limit.** Remove an machine you do not
  use from your Vekrel account's Vekrel Connect page, then restart Vekrel on the host.

## The clock is off

Vekrel Connect signs requests with short-lived proofs. When the clock on this
device or the host is a few minutes wrong, Vekrel Connect rejects them. Turn on
automatic date and time on both devices, then retry.

## The host runs an older Vekrel

This app needs a newer Vekrel on the host. Update Vekrel on the host. See
[Updating Vekrel](./updating.md).

## Vekrel Connect returned an error

| Error                                                     | Fix                                                                                                                                         |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `machine_link_limit_exceeded` or managed tunnel limit | Remove an unused machine, then restart Vekrel on the host.                                                                             |
| `auth_invalid` or `invalid_bearer`                        | Run `vekrel connect login`. If credentials were revoked, run `vekrel connect logout`, then `vekrel connect` again. Restart the server after signing in. |
| Expired or invalid link proof                             | Check the host's date and time, update Vekrel, then restart it.                                                                            |
| HTTP 403 without a recognized error                       | Check relay access, proxies, and firewall rules. Keep any Cloudflare Ray ID for a bug report.                                               |
| HTTP 408, 429, or 5xx                                     | Wait a few minutes. Vekrel retries temporary failures on its own.                                                                          |

After you fix a permanent rejection, restart the host's server with
`vekrel service restart`. For a foreground server, stop it and run `vekrel serve` again.

If Vekrel on the host disappears when SSH closes, see
[background-service troubleshooting](./background-service.md#troubleshooting).

## Report a problem

If none of this helps, open an issue on
[GitHub](https://github.com/vekrel/engine/issues). Include:

- The text from **Copy diagnostics** in the machine's **Details**.
- The output of `vekrel connect status` on the host.
- The trace ID, if one is shown.
