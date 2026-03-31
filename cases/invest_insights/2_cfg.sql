
CREATE MUTABLE STREAM if not exists invest_insights.cfg (
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
PRIMARY KEY securityId;


INSERT INTO invest_insights.cfg 
(
    id, securityId, securityExchange, businessType, securityPosition, 
    minSpread, minReportBalance, avgSingleReportBalance, callAuctionRatio, 
    continousAuctionRatio, execBalanceRequire, execBalanceRatio, 
    timeWeightReportPriceDiff, continousAuctionEffectRatio, 
    lastNoReportPriceTime, canceledReportRatio, canceledNum, 
    singleExecRatio, execAmountExceedHistoryAvgRatio, 
    DeviationQuotationRatio, FutureSpotExposure, MarketMaker
)
SELECT * FROM invest_insights.generate_cfg_data;