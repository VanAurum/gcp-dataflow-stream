python -m pipeline.pipeline \
  --project vanaurum \
  --runner DataflowRunner \
  --staging_location gs://vanaurum-stock-stream/staging \
  --temp_location gs://vanaurum-stock-stream/temp \
  --experiments=allow_non_updatable_job parameter\
--input_mode stream \
--input_topic projects/vanaurum/topics/stock-stream \
--output_table vanaurum:vanaurum_data.stock_stream