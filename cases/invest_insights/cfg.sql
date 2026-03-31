CREATE EXTERNAL STREAM invest_insights.generate_cfg_data(
    id string,
    securityId string,
    securityExchange string,
    businessType string,
    securityPosition float64,
    minSpread float64,
    minReportBalance float64,
    avgSingleReportBalance float64,
    callAuctionRatio float64,
    continousAuctionRatio float64,
    execBalanceRequire float64,
    execBalanceRatio float64,
    timeWeightReportPriceDiff float64,
    continousAuctionEffectRatio float64,
    lastNoReportPriceTime string,
    canceledReportRatio float64,
    canceledNum float64,
    singleExecRatio float64,
    execAmountExceedHistoryAvgRatio float64,
    DeviationQuotationRatio float64,
    FutureSpotExposure string,
    MarketMaker string
)
AS $$
import random

def generate_records():
    # The base record template
    record_template = [
        '1879', '', '1', '4', 100.0,
        0.08, 1.0, 0.0, 0.0, 0.0,
        0.0, 0.0, 0.0, 0.0, '0',
        0.0, 0.0, 0.0, 0.0, 0.0,
        '0', 'D890088888, 6565656565, D890054005'
    ]

    # Generate rows and yield them
    for i in range(100001, 103000):
        new_record = record_template.copy()
        new_record[1] = str(i)                      # securityId
        new_record[5] = random.uniform(0.1, 0.9)    # minSpread
        new_record[6] = random.uniform(100, 10000)  # minReportBalance
        
        # Explicitly converting the list to a tuple for the engine
        yield tuple(new_record)
$$
SETTINGS type='python',
         read_function_name='generate_records';