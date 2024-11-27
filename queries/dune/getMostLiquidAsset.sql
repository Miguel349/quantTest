SELECT
  blockchain,
  src_token_symbol,
  dst_token_symbol,
  COUNT(src_token_symbol) AS operation_count
FROM oneinch.swaps
WHERE
  flags['fusion']
GROUP BY blockchain, src_token_symbol, dst_token_symbol
ORDER BY operation_count DESC;