"""Context acceptance for the current single shared gateway architecture.

The retired probe assumed separate processes and tokens for each account.
Account isolation now lives in the authenticated portal and private Agents;
the trusted native gateway token is never an end-user access credential.
This launcher checks private context, model switching and restart recovery
using synthetic providers. Long synthetic turns trigger native automatic
compaction, and retained memory is checked again after a gateway restart.
"""
import argparse
import asyncio

from check_lighthouse_shared_gateway import run

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--accounts', type=int, default=2)
    asyncio.run(run(parser.parse_args().accounts, compaction=True))
