from pyflink.common import WatermarkStrategy
from java.time import Duration

# Required watermark strategy exactly as specified
WATERMARK_STRATEGY = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))
