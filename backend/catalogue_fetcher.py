import time

from graphql_client import gql

SEARCH_PAGE_SIZE = 200
DETAIL_BATCH_SIZE = 100
REQUEST_DELAY_S = 0.1

SEARCH_QUERY = """\
query SearchSKUs($input: ProductSearchInput!) {
  searchProducts(input: $input) {
    count
    products {
      sku
    }
  }
}
"""

DETAIL_QUERY = """\
query GetProductDetail($skus: [String!]!) {
  products(skus: $skus) {
    sku
    title
    brand
    ean
    rootCategory
    packSizeDisplay
    description
    storage
    ingredients
    inAssortment
    isMedicine
    retailSet
    image
    productAllergens {
      contains
      mayContain
    }
    nutritionsTable {
      columns
      rows
    }
    nutriScore {
      value
    }
    availability {
      isAvailable
      availability
    }
    price {
      price
      promoPrice
      pricePerUnit {
        price
        unit
      }
    }
    categories {
      name
      path
      id
    }
    primaryProductBadges {
      alt
      image
    }
    secondaryProductBadges {
      alt
      image
    }
  }
}
"""


def fetch_all_skus(limit: int | None = None) -> list[str]:
    """Fetch every SKU from the full catalogue using paginated wildcard search."""
    skus: list[str] = []
    offset = 0

    print("Fetching SKUs from catalogue...")
    while True:
        batch_limit = (
            min(SEARCH_PAGE_SIZE, limit - len(skus)) if limit else SEARCH_PAGE_SIZE
        )
        result = gql(
            "SearchSKUs",
            SEARCH_QUERY,
            {
                "input": {
                    "searchTerms": "*",
                    "searchType": "keyword",
                    "limit": batch_limit,
                    "offSet": offset,
                }
            },
        )

        sp = (result.get("data") or {}).get("searchProducts") or {}
        total = sp.get("count", 0)
        page_skus = [p["sku"] for p in sp.get("products", [])]

        if not page_skus:
            break

        skus.extend(page_skus)
        offset += len(page_skus)

        print(f"  {len(skus):>6} / {total} SKUs fetched", end="\r")

        if limit and len(skus) >= limit:
            break
        if len(skus) >= total:
            break

        time.sleep(REQUEST_DELAY_S)

    print(f"\nTotal SKUs fetched: {len(skus)}")
    return skus


def fetch_product_details(skus: list[str]) -> list[dict]:
    """Fetch full product detail for a list of SKUs in batches of DETAIL_BATCH_SIZE."""
    products: list[dict] = []

    for i in range(0, len(skus), DETAIL_BATCH_SIZE):
        batch = skus[i : i + DETAIL_BATCH_SIZE]
        result = gql("GetProductDetail", DETAIL_QUERY, {"skus": batch})

        errors = result.get("errors", [])
        if errors:
            print(
                f"  Warning: GraphQL errors for batch {i}-{i+len(batch)}: "
                f"{errors[0].get('message', '')[:120]}"
            )

        batch_products = result.get("data", {}).get("products", [])
        products.extend(p for p in batch_products if p)

        done = min(i + DETAIL_BATCH_SIZE, len(skus))
        print(f"  Details fetched: {done:>6} / {len(skus)}", end="\r")

        time.sleep(REQUEST_DELAY_S)

    print(f"\nTotal product details fetched: {len(products)}")
    return products
