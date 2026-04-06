import type { StreamFn } from "@mariozechner/pi-agent-core";
import type { AssistantMessage } from "@mariozechner/pi-ai";
import { log } from "../logger.js";

const RETRY_DELAY_MS = 2000;
const MAX_RETRIES = 6;
const MAX_CONCURRENT = 3;

let activeStreams = 0;
const queue: (() => void)[] = [];

async function acquireToken(): Promise<void> {
  if (activeStreams < MAX_CONCURRENT) {
    activeStreams++;
    return;
  }
  return new Promise((resolve) => queue.push(resolve));
}

function releaseToken(): void {
  if (queue.length > 0) {
    const next = queue.shift();
    if (next) {
      next();
    }
  } else {
    activeStreams--;
  }
}

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRateLimitError(error: unknown): boolean {
  if (!error) {
    return false;
  }
  const msg =
    error instanceof Error
      ? error.message
      : typeof error === "string"
        ? error
        : JSON.stringify(error);
  const lower = msg.toLowerCase();
  return (
    lower.includes("429") ||
    lower.includes("rate limit") ||
    lower.includes("too many requests") ||
    lower.includes("quota exceeded")
  );
}

export function wrapStreamFnWithRetryAndConcurrency(baseFn: StreamFn): StreamFn {
  return (...args: Parameters<StreamFn>) => {
    const [model, context, options] = args;
    return {
      async *[Symbol.asyncIterator]() {
        await acquireToken();
        try {
          let attempt = 0;
          while (true) {
            try {
              // Note: baseFn initiates the API call
              const streamTarget = await Promise.resolve(baseFn(model, context, options));
              // eslint-disable-next-line @typescript-eslint/await-thenable
              const iterator = streamTarget[Symbol.asyncIterator]();
              while (true) {
                const result = await iterator.next();
                if (result.done) {
                  return result.value;
                }
                yield result.value;
              }
            } catch (error: unknown) {
              if (isRateLimitError(error)) {
                attempt++;
                if (attempt > MAX_RETRIES) {
                  log.error(
                    `[StreamRetry] Exceeded max retries (${MAX_RETRIES}) for Rate Limit error.`,
                  );
                  throw error;
                }
                const backoff = RETRY_DELAY_MS * Math.pow(2, attempt - 1) + Math.random() * 1000;
                log.warn(
                  `[StreamRetry] Rate limit hit. Retrying attempt ${attempt}/${MAX_RETRIES} after ${Math.floor(backoff)}ms...`,
                );
                await sleep(backoff);
                continue;
              }
              throw error;
            }
          }
        } finally {
          releaseToken();
        }
      },
      async result(): Promise<AssistantMessage> {
        await acquireToken();
        try {
          let attempt = 0;
          while (true) {
            try {
              const streamTarget = await Promise.resolve(baseFn(model, context, options));
              return await streamTarget.result();
            } catch (error: unknown) {
              if (isRateLimitError(error)) {
                attempt++;
                if (attempt > MAX_RETRIES) {
                  log.error(
                    `[StreamRetry] Exceeded max retries (${MAX_RETRIES}) for Rate Limit error on result().`,
                  );
                  throw error;
                }
                const backoff = RETRY_DELAY_MS * Math.pow(2, attempt - 1) + Math.random() * 1000;
                log.warn(
                  `[StreamRetry] Rate limit hit on .result(). Retrying attempt ${attempt}/${MAX_RETRIES} after ${Math.floor(backoff)}ms...`,
                );
                await sleep(backoff);
                continue;
              }
              throw error;
            }
          }
        } finally {
          releaseToken();
        }
      },
    } as unknown as StreamFn;
  };
}
