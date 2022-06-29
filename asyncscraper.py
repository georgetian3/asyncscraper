import os
import aiohttp
import asyncio
from tqdm import tqdm

class AsyncScraper:

    def __init__(self, parser=None, url_creator=None):
        self._url_creator = url_creator
        self._parser = parser

    def _url_to_filename(self, url):
        invalid = set(r'\/:*?"<>|')
        return ''.join(char for char in url if char not in invalid)

    async def _download(self, url):
        data = None
        if self._cache:
            try:
                with open(f'.cache/{self._url_to_filename(url)}', 'rb') as f:
                    data = f.read()
            except FileNotFoundError:
                pass
        if not data:
            async with self._session.get(url) as response:
                if response.status == 200:
                    data = await response.read()
                if data and self._cache:
                    with open(f'.cache/{self._url_to_filename(url)}', 'wb') as f:
                        f.write(data)
        return data

    async def _call(self, key):
        response = await self._download(self._url_creator(key) if self._url_creator else key)
        if response:
            if self._parser:
                try:
                    response = self._parser(key, response)
                except Exception as e:
                    print('Parsing error\nKey:', key, '\nException:', e)
                    response = None
            self._data[key] = response
        if self._verbose:
            self._pbar.update()

    async def _run(self):
        conn = aiohttp.TCPConnector()
        self._session = aiohttp.ClientSession(connector=conn)
        if self._verbose:
            self._pbar = tqdm(range(len(self._keys)))

        await asyncio.gather(*(self._call(key) for key in self._keys))

        await self._session.close()
        if self._verbose:
            self._pbar.close()

    def run(self, keys, verbose=True, cache=False):
        if cache:
            Path('.cache').mkdir(exist_ok=True)
        self._verbose = verbose
        self._cache = cache
        self._keys = set(keys)
        self._data = {}
        asyncio.run(self._run())
        return self._data