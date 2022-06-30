from pathlib import Path
from tqdm import tqdm
import aiohttp
import asyncio

class AsyncScraper:
    async def _download(self, url):
        if self._redownload or not Path(url[1]).is_file():
            while True:
                try:
                    async with self._session.get(url[0]) as response:
                        if response.status == 200:
                            data = await response.read()
                        else:
                            print(url, response.status)
                        if data:
                            with open(url[1], 'wb') as f:
                                f.write(data)
                    break
                except:
                    continue
        if self._verbose:
            self._pbar.update()

    async def _run(self, urls):
        conn = aiohttp.TCPConnector()
        self._session = aiohttp.ClientSession(connector=conn)
        if self._verbose:
            self._pbar = tqdm(range(len(urls)))

        await asyncio.gather(*(self._download(url) for url in urls))

        await self._session.close()
        if self._verbose:
            self._pbar.close()

    def run(self, urls, verbose=True, redownload=False):
        self._verbose = verbose
        self._redownload = redownload
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(self._run(urls))

if __name__ == '__main__':
    pass