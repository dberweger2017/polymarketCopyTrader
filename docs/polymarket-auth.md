# Polymarket Auth And Wallet Roles

This document explains which wallet and credentials to use for each Polymarket API action in this repository.

It reflects the account setup currently configured in [`.env`](/Users/dberweger/Desktop/Code/polymarketCopyTrader/.env).

## Short Version

- Use the exported private key in `POLYMARKET_PRIVATE_KEY` to sign requests and orders.
- Use `POLYMARKET_SIGNATURE_TYPE=1` for this account.
- Use `POLYMARKET_FUNDER_ADDRESS` as the trading wallet that actually holds cash and positions.
- Use `POLYMARKET_API_KEY`, `POLYMARKET_API_SECRET`, and `POLYMARKET_API_PASSPHRASE` for authenticated CLOB requests.
- Use the `POLYMARKET_BUILDER_*` credentials only for builder attribution and builder analytics. They are not the user trading credentials.

## Account Mapping For This Repo

Current addresses:

- Signer / exported private-key wallet: `0x171768E4804ac2b25CA54e405D9f1DF8E5eb8d9E`
- Proxy / funder / trading wallet: `0x288d3c0a69966ef5b8b6549232e5b8e52b2cf517`

Validated behavior for this account:

- The funded trading identity is the proxy wallet `0x288d...cf517`.
- The correct Polymarket signature type is `1` (`POLY_PROXY`).
- The private key corresponds to the signer wallet and is used for L1 auth and order signing.
- The proxy wallet is the wallet that should be queried for positions, activity, and trading balance.

## Polymarket Auth Layers

Polymarket’s CLOB uses two different auth layers:

### L1 Auth

Used for:

- creating or deriving user API credentials
- proving control of the private key
- signing order payloads locally

Inputs:

- private key
- signer address

In this repo:

- `POLYMARKET_PRIVATE_KEY`
- `POLYMARKET_SIGNER_ADDRESS`

### L2 Auth

Used for:

- `GET /balance-allowance`
- `GET /data/orders`
- `GET /data/trades`
- `POST /order`
- `DELETE /order`
- other authenticated CLOB endpoints

Inputs:

- user API key
- user API secret
- user API passphrase
- signature type
- funder address
- private key, because orders are still signed locally even when L2 headers are used

In this repo:

- `POLYMARKET_API_KEY`
- `POLYMARKET_API_SECRET`
- `POLYMARKET_API_PASSPHRASE`
- `POLYMARKET_SIGNATURE_TYPE`
- `POLYMARKET_FUNDER_ADDRESS`
- `POLYMARKET_PRIVATE_KEY`

## What Each Wallet Is For

### Signer Wallet

Use the signer wallet for:

- deriving user API credentials
- creating authenticated CLOB clients
- signing orders before submission

Do not use the signer wallet address as the source of truth for:

- current Polymarket positions
- current Polymarket activity
- available trading cash

For this account, those live under the proxy wallet instead.

### Proxy / Funder Wallet

Use the proxy wallet for:

- balance checks
- positions
- activity history
- trade ownership
- order ownership
- the `funder` argument when initializing the CLOB client

For this account, this is the wallet that actually holds the approximately `$50` trading balance.

### Builder Credentials

Use builder credentials only for:

- order attribution
- builder trade analytics
- builder leaderboard / reward tracking

Do not use builder credentials for:

- user balance checks
- user positions
- user open orders
- user trade history
- basic trading auth by themselves

Builder credentials are separate from the user L2 credentials.

## Correct Repo Configuration

The effective config for this account is:

```env
POLYMARKET_SIGNATURE_TYPE=1
POLYMARKET_FUNDER_ADDRESS=0x288d3c0a69966ef5b8b6549232e5b8e52b2cf517
POLYMARKET_PRIVATE_KEY=<user private key>
POLYMARKET_API_KEY=<user l2 api key>
POLYMARKET_API_SECRET=<user l2 api secret>
POLYMARKET_API_PASSPHRASE=<user l2 api passphrase>
```

Optional builder config:

```env
POLYMARKET_BUILDER_API_KEY=<builder key>
POLYMARKET_BUILDER_API_SECRET=<builder secret>
POLYMARKET_BUILDER_API_PASSPHRASE=<builder passphrase>
```

## Which Address To Query

Use these rules:

- Data API `/activity`: query the proxy wallet
- Data API `/positions`: query the proxy wallet
- Data API `/value`: query the proxy wallet
- CLOB `get_balance_allowance()`: use the configured L2 client with `signature_type=1` and `funder=proxy wallet`
- CLOB `get_orders()`: same L2 client
- CLOB `get_trades()`: same L2 client
- live order placement: same L2 client, with orders signed by the private key

## Python Client Example

```python
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds

creds = ApiCreds(
    api_key=POLYMARKET_API_KEY,
    api_secret=POLYMARKET_API_SECRET,
    api_passphrase=POLYMARKET_API_PASSPHRASE,
)

client = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key=POLYMARKET_PRIVATE_KEY,
    creds=creds,
    signature_type=1,
    funder=POLYMARKET_FUNDER_ADDRESS,
)
```

## Common Failure Mode

If balance, positions, or open orders come back as zero even though the Polymarket UI shows funds, the usual cause is one of these:

- wrong `signature_type`
- wrong `funder` address
- querying the signer wallet instead of the proxy wallet
- mixing builder credentials with user trading credentials

For this account, the wrong configuration was `signature_type=2`. The correct configuration is `signature_type=1`.

## Security Notes

- Do not commit raw private keys or API secrets into the repository.
- Keep secrets only in `.env` or another local secret store.
- Builder credentials and user credentials should be treated as separate secrets.
- If secrets have been pasted into chat or committed anywhere by mistake, rotate them.

## References

- [Polymarket Authentication](https://docs.polymarket.com/api-reference/authentication)
- [Polymarket L2 Client Methods](https://docs.polymarket.com/trading/clients/l2)
- [Polymarket Order Attribution](https://docs.polymarket.com/trading/orders/attribution)
- [Polymarket Positions API](https://docs.polymarket.com/api-reference/core/get-current-positions-for-a-user)
- [Polymarket User Position Value API](https://docs.polymarket.com/api-reference/core/get-total-value-of-a-users-positions)
