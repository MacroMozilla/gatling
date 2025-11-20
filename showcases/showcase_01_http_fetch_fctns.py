from gatling.utility.http_fetch_fctns import sync_fetch_http, async_fetch_http, fwrap
import asyncio

target_url = "https://httpbin.org/get"
# --- Synchronous request ---
result, status, size = sync_fetch_http(target_url)
print(status, size, result[:80])


# --- Asynchronous request ---
async def main():
    res, status, size = await fwrap(async_fetch_http, target_url=target_url, rtype="json")
    print(res)


asyncio.run(main())
