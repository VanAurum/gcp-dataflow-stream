python -m pipeline.pipeline \
  --project vanaurum \
  --runner DataflowRunner \
  --staging_location gs://vanaurum-stock-stream/staging \
  --temp_location gs://vanaurum-stock-stream/temp \
--input_mode stream \
--input_topic projects/vanaurum/topics/stock-stream \
--output gs://vanaurum-stock-stream