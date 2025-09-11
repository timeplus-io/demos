from proton_driver import client

import random

host = "timeplusd-svc.timeplus"
user = "admin"
password = "Password!"
c = client.Client(host=host, port=8463, user=user, password=password)

record = [
    # id, securityId, securityExchange, businessType, securityPosition,
    '1879', '188001', '1', '4', 100,
    # minSpread, minReportBalance, avgSingleReportBalance, callAuctionRatio, continousAuctionRatio, ,
    0.08, 1, 0, 0, 0,
    # execBalanceRequire, execBalanceRatio, timeWeightReportPriceDiff, continousAuctionEffectRatio,
    # lastNoReportPriceTime,
    0, 0, 0, 0, '0',
    # canceledReportRatio, canceledNum, singleExecRatio, execAmountExceedHistoryAvgRatio, DeviationQuotationRatio,
    0, 0, 0, 0, 0,
    # FutureSpotExposure, MarketMaker
    '0', 'D890088888, 6565656565, D890054005'
]

data = {
        "columns": [
            'id', 'securityId', 'securityExchange', 'businessType', 'securityPosition',
            'minSpread', 'minReportBalance', 'avgSingleReportBalance', 'callAuctionRatio', 'continousAuctionRatio',
            'execBalanceRequire', 'execBalanceRatio', 'timeWeightReportPriceDiff', 'continousAuctionEffectRatio', 'lastNoReportPriceTime',
            'canceledReportRatio', 'canceledNum', 'singleExecRatio', 'execAmountExceedHistoryAvgRatio', 'DeviationQuotationRatio',
            'FutureSpotExposure', 'MarketMaker'
        ]
    }
rows = []
for i in range(100001, 103000):
    new_record = record.copy()
    new_record[1] = f"{i}"
    new_record[5] = random.uniform(0.1, 0.9)
    new_record[6] = random.uniform(100, 10000)
    rows.append(new_record)
    
cols = ','.join(data["columns"])
sql = f"INSERT INTO invest_insights.cfg ({cols})"
c.execute(f"INSERT INTO invest_insights.cfg ({cols}) VALUES", rows)