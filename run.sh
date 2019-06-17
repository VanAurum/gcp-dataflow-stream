python -m correlated_trading.trading_pipe \
  --project vanaurum \
  --runner DataflowRunner \
  --staging_location gs:// \
  --temp_location gs://vanaurum-stock-stream/ \
--input_mode stream \
--input_topic stock-stream \
  --output gs://vanaurum-stock-stream/