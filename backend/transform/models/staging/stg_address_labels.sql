-- P2.2: staging view over raw_address_labels (CryptoLabel lookups; has_label
-- marks attempts with a real entity name — entity_name 'Unknown' counts as
-- unlabeled and is retried by the fetcher until CryptoLabel identifies it).
select
    address,
    entity_name,
    entity_category,
    address_type,
    explorer_url,
    labels,
    (entity_name is not null and entity_name <> 'Unknown') as has_label,
    source,
    fetched_at
from {{ source('raw', 'raw_address_labels') }}
