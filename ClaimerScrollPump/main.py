import json
import random
import asyncio
import aiohttp
import pandas as pd
from web3 import Web3
from sys import stderr
from loguru import logger
from web3.eth import AsyncEth

from settings import delay_wallets, ref_address

logger.remove()
logger.add(stderr,
           format="<lm>{time:HH:mm:ss}</lm> | <level>{level}</level> | <blue>{function}:{line}</blue> "
                  "| <lw>{message}</lw>")

with open('abi.json') as file:
    ABI_CLAIMER = json.load(file)


class Claimer:
    def __init__(self, private_key: str, proxy: str, number_acc: int) -> None:
        self.proxy: str = f"http://{proxy}" if proxy is not None else None
        self.private_key = private_key
        self.id: int = number_acc
        self.rpc: str = 'https://scroll.drpc.org'
        self.scan: str = 'https://scrollscan.com/tx/'
        self.client = None
        self.w3 = Web3(
            provider=Web3.AsyncHTTPProvider(endpoint_uri=self.rpc),
            modules={"eth": AsyncEth},
            middlewares=[])
        if proxy is not None:
            self.web3 = Web3(
                provider=Web3.AsyncHTTPProvider(endpoint_uri=self.rpc,
                                                request_kwargs={"proxy": self.proxy}),
                modules={"eth": AsyncEth},
                middlewares=[])

        self.account = self.w3.eth.account.from_key(private_key=private_key)

    async def claim(self):
        async with aiohttp.ClientSession(headers={
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'origin': 'https://scrollpump.xyz',
            'priority': 'u=1, i',
            'referer': 'https://scrollpump.xyz/',
            'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/128.0.0.0 Safari/537.36',
        }) as client:
            self.client: aiohttp.ClientSession = client

            response: aiohttp.ClientResponse = await self.client.get(
                url=f'https://api.scrollpump.xyz/api/Airdrop/GetReward',
                params={
                    'address': self.account.address,
                },
                proxy=self.proxy
            )
            response_json: dict = await response.json()
            reward: int = response_json['data']['baseReward'] + response_json['data']['bonusReward']

            if int(reward) == 0:
                logger.info(f'{self.account.address}: 0 tokens for claim')
                return

            response: aiohttp.ClientResponse = await self.client.get(
                url=f'https://api.scrollpump.xyz/api/Airdrop/GetSign',
                params={
                    'address': self.account.address,
                },
                proxy=self.proxy
            )
            response_json: dict = await response.json()
            sign: str = response_json['data']['sign']
            amount: int = response_json['data']['amount']

            contract = self.w3.eth.contract(
                address=Web3.to_checksum_address('0xCe64dA1992Cc2409E0f0CdCAAd64f8dd2dBe0093'),
                abi=ABI_CLAIMER)

            transaction = await contract.functions.claim(
                int(amount),
                sign,
                Web3.to_checksum_address(ref_address)
            ).build_transaction({
                "from": self.account.address,
                "gasPrice": int(await self.w3.eth.gas_price * 1.05),
                'chainId': 534352,
                "nonce": await self.w3.eth.get_transaction_count(self.account.address),
                "value": 0,
                "gas": 0
            })

            try:
                transaction["gas"] = int(await self.w3.eth.estimate_gas(transaction) * 1.2)

                signed_txn = self.w3.eth.account.sign_transaction(transaction, self.private_key)

                tx_hash = await self.w3.eth.send_raw_transaction(signed_txn.raw_transaction)
                tx_hash = self.w3.to_hex(tx_hash)
                while True:
                    try:
                        receipt = await self.w3.eth.get_transaction_receipt(tx_hash)
                        break
                    except Exception:
                        await asyncio.sleep(5)

                if receipt['status'] == 1:
                    logger.success(f'{self.account.address}: Success claim {reward} PUMP: {self.scan + tx_hash}')
                    return reward

                else:
                    logger.error(f'{self.account.address}: Failed claim {reward} PUMP: {self.scan + tx_hash}')
                    raise ValueError("Transaction Failed")

            except Exception as e:
                if 'Tokens have already been claimed by this address.' in str(e):
                    logger.info(f'{self.account.address}: Tokens have already been claimed by this address.')
                    return reward
                raise Exception(f'Error: {str(e)}')


async def start_claim(account: list, id_acc: int, semaphore) -> None:
    async with semaphore:
        acc = Claimer(private_key=account[0], proxy=account[1], number_acc=id_acc)
        try:

            tokens = await acc.claim()

        except Exception as e:
            logger.error(f'{id_acc} Failed: {str(e)}')

        sleep_time = random.randint(delay_wallets[0], delay_wallets[1])
        if sleep_time != 0:
            logger.info(f'Sleep {sleep_time} sec...')
            await asyncio.sleep(sleep_time)
        return tokens if tokens is int else 0


async def main() -> None:
    semaphore: asyncio.Semaphore = asyncio.Semaphore(1)

    tasks: list[asyncio.Task] = [
        asyncio.create_task(coro=start_claim(account=account, id_acc=idx, semaphore=semaphore))
        for idx, account in enumerate(accounts, start=1)
    ]
    total_tokens: float = sum(list(await asyncio.gather(*tasks)))
    print()
    logger.info(f'Total tokens: {total_tokens}')


if __name__ == '__main__':
    with open('accounts_data.xlsx', 'rb') as file:
        exel = pd.read_excel(file)

    accounts: list[list] = [
        [
            row["Private Key"],
            row["Proxy"] if isinstance(row["Proxy"], str) else None
        ]
        for index, row in exel.iterrows()
    ]

    logger.info(f'My channel: https://t.me/CryptoMindYep')
    logger.info(f'Total wallets: {len(accounts)}\n')
    asyncio.run(main())

    logger.info('The work completed')
    logger.info('Thx for donat: 0x5AfFeb5fcD283816ab4e926F380F9D0CBBA04d0e')
