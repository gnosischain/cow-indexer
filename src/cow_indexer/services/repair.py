from cow_indexer.services.historical import HistoricalIndexer


async def repair_range(indexer: HistoricalIndexer, from_block: int, to_block: int) -> int:
    return await indexer.scan(
        from_block,
        to_block,
        update_checkpoint=False,
        store_all_blocks=True,
    )
