/**
 * The Telegram Connector is a scheduled Cloudflare Worker that fetches messages from Telegram channels and stores them by running a container using `env.CONTAINER`
 * 
 * ## Best Practices
 * - Simplicity, reliability, and efficiency.
 * - Stick to JSDoc for specifications, documentation, and type definitions.
 * - Robust error handling and logging techniques with succinct messages. Wrapping each data processing stage in a try-catch block, validating all inputs and outputs, and using `INFO` and `ERROR` levels with detailed contextual information, such as processing stage, task name, etc.
 * - Concise code with minimal formatting and indentation, which prioritizes descriptive element naming and log messages over inline comments to achieve readability.
 * 
 * ## Additional Documentation
 *
 * ### Containers in Cloudflare Worker
 * ```js
 * import { Container, getContainer } from "@cloudflare/containers";
 *
 * export class MyContainer extends Container {
 *   defaultPort = 4000; // Port the container is listening on
 *   sleepAfter = "10m"; // Stop the instance if requests not sent for 10 minutes
 * }
 *
 * export default {
 *   async fetch(request, env) {
 *     const { "session-id": sessionId } = await request.json();
 *     // Get the container instance for the given session ID
 *     const containerInstance = getContainer(env.MY_CONTAINER, sessionId);
 *     // Pass the request to the container instance on its default port
 *     return containerInstance.fetch(request);
 *   },
 * };
 * ```
 */

import { Container, getContainer } from "@cloudflare/containers"
import { env } from "cloudflare:workers"

export class MyContainer extends Container {
  defaultPort = 8080
  sleepAfter = "10s"

  envVars = {
    TIMESCALE_CONNECTION: env.TIMESCALE_CONNECTION,
  }
}

/**
 * Parses account configurations from environment variables.
 * Supports both multi-account (ACCOUNTS_CONFIG JSON) and single account (legacy env vars).
 * @param {Env} env - Cloudflare environment variables
 * @returns {Array<Object>} Array of account configurations
 */
function getAccountConfigs(env) {
  if (env.ACCOUNTS_CONFIG) {
    try {
      const accounts = JSON.parse(env.ACCOUNTS_CONFIG)
      if (Array.isArray(accounts) && accounts.length > 0) {
        console.log(`INFO: Found ${accounts.length} account(s) in ACCOUNTS_CONFIG`)
        return accounts
      }
    } catch (e) {
      console.error(`ERROR: Failed to parse ACCOUNTS_CONFIG: ${e.message}`)
    }
  }

  if (env.TELEGRAM_API_ID && env.TELEGRAM_API_HASH && env.TELEGRAM_SESSION_STR) {
    console.log("INFO: Using legacy single account configuration")
    return [{
      accountId: env.TELEGRAM_API_ID,
      apiId: env.TELEGRAM_API_ID,
      apiHash: env.TELEGRAM_API_HASH,
      sessionStr: env.TELEGRAM_SESSION_STR,
    }]
  }

  throw new Error("No account configuration found. Provide either ACCOUNTS_CONFIG or legacy TELEGRAM_* env vars")
}

async function processAccounts(env) {
  try {
    const accountConfigs = getAccountConfigs(env)
    const results = []

    for (const account of accountConfigs) {
      try {
        const accountId = account.accountId || account.apiId
        console.log(`INFO: Processing account ${accountId}`)

        const containerInstance = getContainer(env.CONTAINER, accountId)
        const requestBody = JSON.stringify({
          accountId: account.accountId || account.apiId,
          apiId: account.apiId,
          apiHash: account.apiHash,
          sessionStr: account.sessionStr,
        })

        const request = new Request("https://example.com/", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: requestBody,
        })

        const response = await containerInstance.fetch(request)
        const responseText = await response.text()

        if (response.ok) {
          console.log(`INFO: Account ${accountId} processed successfully`)
          results.push({ accountId, status: "success" })
        } else {
          console.error(`ERROR: Account ${accountId} failed with status ${response.status}: ${responseText}`)
          results.push({ accountId, status: "error", error: responseText })
        }
      } catch (accountError) {
        console.error(`ERROR: Failed to process account ${account.accountId || account.apiId}: ${accountError.message}`)
        results.push({ accountId: account.accountId || account.apiId, status: "error", error: accountError.message })
      }
    }

    const successCount = results.filter(r => r.status === "success").length
    const totalCount = results.length

    return new Response(
      JSON.stringify({ message: `Processed ${successCount}/${totalCount} accounts`, results }),
      { status: successCount === totalCount ? 200 : 207, headers: { "Content-Type": "application/json" } }
    )
  } catch (e) {
    console.error(`ERROR: Error processing accounts: ${e.message}`)
    return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: { "Content-Type": "application/json" } })
  }
}

export default {
  async fetch(request, env, ctx) {
    return await processAccounts(env)
  },
  async scheduled(ctx, env) {
    return await processAccounts(env)
  },
}
