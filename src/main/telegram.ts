/**
 * TelegramGroupBridge — poll a Telegram group and relay messages to the harness.
 *
 * Uses the Telegram Bot API (REST long-polling) so no extra npm dependencies are
 * needed (Node 18+ fetch is sufficient). One bot token reads + posts to the group;
 * other bots (Dwight, Jim, Pam…) are managed by the OpenClaw gateway separately.
 *
 * Flow:
 *   group message arrives → getUpdates long-poll fires → onMessage(msg) callback
 *   harness wants to send → sendMessage(text) → POST to Telegram
 *
 * Runs in the Electron main process. No electron imports so it stays testable.
 */

const TELEGRAM_API = 'https://api.telegram.org';
/** Long-poll timeout in seconds (Telegram max is 50). */
const POLL_TIMEOUT_SEC = 30;
/** Delay before retrying after a network error (ms). */
const RETRY_DELAY_MS = 5_000;

export interface TelegramGroupMessage {
  messageId: number;
  fromName: string;
  fromUsername?: string;
  isBot: boolean;
  text: string;
  date: number; // unix seconds
}

export interface TelegramGroupBridgeOptions {
  /** ClawedMichaelBot (or any bot in the group) token for polling + sending. */
  botToken: string;
  /** The group's chat_id (negative integer, e.g. -5254720802). */
  groupChatId: number;
  /** Called for each new message received from the group. */
  onMessage: (msg: TelegramGroupMessage) => void;
  /** Called when the polling loop errors (optional, for logging). */
  onError?: (err: unknown) => void;
}

export class TelegramGroupBridge {
  private readonly token: string;
  private readonly chatId: number;
  private readonly onMessage: (msg: TelegramGroupMessage) => void;
  private readonly onError?: (err: unknown) => void;

  private offset = 0;
  private running = false;
  private abortController: AbortController | null = null;

  constructor(opts: TelegramGroupBridgeOptions) {
    this.token = opts.botToken;
    this.chatId = opts.groupChatId;
    this.onMessage = opts.onMessage;
    this.onError = opts.onError;
  }

  /** Begin long-polling. Safe to call multiple times (idempotent). */
  start(): void {
    if (this.running) return;
    this.running = true;
    void this.loop();
  }

  /** Stop polling. Aborts any in-flight request. */
  stop(): void {
    this.running = false;
    try { this.abortController?.abort(); } catch { /* ignore */ }
    this.abortController = null;
  }

  /** Send a message to the group as this bot. */
  async sendMessage(text: string): Promise<{ ok: boolean; error?: string }> {
    try {
      const res = await fetch(`${TELEGRAM_API}/bot${this.token}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: this.chatId, text, parse_mode: 'Markdown' })
      });
      const json = await res.json() as { ok: boolean; description?: string };
      return json.ok ? { ok: true } : { ok: false, error: json.description };
    } catch (e) {
      return { ok: false, error: e instanceof Error ? e.message : String(e) };
    }
  }

  // ─── internals ──────────────────────────────────────────────────────────────

  private url(method: string): string {
    return `${TELEGRAM_API}/bot${this.token}/${method}`;
  }

  private async loop(): Promise<void> {
    while (this.running) {
      try {
        this.abortController = new AbortController();
        const res = await fetch(this.url('getUpdates'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            offset: this.offset,
            timeout: POLL_TIMEOUT_SEC,
            allowed_updates: ['message']
          }),
          signal: this.abortController.signal
        });

        if (!res.ok) {
          this.onError?.(new Error(`getUpdates HTTP ${res.status}`));
          await this.delay(RETRY_DELAY_MS);
          continue;
        }

        const json = await res.json() as {
          ok: boolean;
          result: Array<{
            update_id: number;
            message?: {
              message_id: number;
              date: number;
              chat: { id: number };
              from?: { first_name?: string; username?: string; is_bot?: boolean };
              text?: string;
            };
          }>;
        };

        if (!json.ok) { await this.delay(RETRY_DELAY_MS); continue; }

        for (const update of json.result) {
          // Advance offset so we never re-process this update.
          this.offset = update.update_id + 1;

          const msg = update.message;
          if (!msg?.text || msg.chat.id !== this.chatId) continue;

          this.onMessage({
            messageId: msg.message_id,
            fromName: msg.from?.first_name ?? 'unknown',
            fromUsername: msg.from?.username,
            isBot: msg.from?.is_bot ?? false,
            text: msg.text,
            date: msg.date
          });
        }
      } catch (e) {
        // AbortError = stop() was called — exit cleanly.
        if (e instanceof Error && e.name === 'AbortError') break;
        this.onError?.(e);
        await this.delay(RETRY_DELAY_MS);
      }
    }
  }

  private delay(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }
}
