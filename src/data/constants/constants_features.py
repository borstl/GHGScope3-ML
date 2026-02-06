REPRESENTATIVE_INFO: list[str] = [
    # Contact, Legal - HQ address is more important than contact address
    'Contact Postal Code',  #TR.CompanyCntPostalCodeAddr
    'Contact Country Name',  #TR.CompanyCntName, TR.CompanyCntCountryAddr
    'Contact Street Address',  #TR.CompanyCntStreetAddr
    'Contact City Address',  #TR.CompanyCntCityAddr
    'Contact Email Address',  #TR.CompanyCntEmail
    'Contact Name',
    'Contact Title',  #TR.CompanyCntTitle
    'Contact Phone Number',  #TR.CompanyPhoneCnt
    'TR.LegalAddressCity',
    'TR.CmnLegalAddressLine1',
    'TR.CmnLegalAddressLine2',
    'TR.CmnLegalAddressLine3',
    'TR.LegalAddressLine1',
    'TR.LegalAddressLine2',
    'TR.LegalAddressLine3',
    'TR.LegalAddressPostalCode',
    'TR.LegalAddressStateProvince',
    'TR.RegistrationCountry',
    'TR.RegistrationCity',
]
CONTACT_INFO: list[str] = [
    # Mail, Telephone, etc. - Unnecessary information
    'Main Fax Number',  #TR.CompanyPhoneFax
    'Main Phone Number',  #TR.CompanyPhoneMain
    'TR.HeadquartersPhone',
    'Web Link to Home Page',  #TR.CompanyLinkHome
    'TR.CompanyEmail',
    'TR.OrganizationWebsite',
]
IDENTIFIER_INFO: list[str] = [
    # Identifier - Redundant information, because we use the RIC (Refinitiv instrument code) as id for financial instruments
    'TR.CommonName',
    'Company Name',  #TR.CompanyName
    'TR.AlsoKnownAsName',
    'TR.FormerlyKnownAsName',
    'TR.InstrumentName',
    'TR.InstrumentCommonName',
    'TR.ExchangeName',
    'TR.ShortExchangeName',
    'TR.OrganizationName',
    'TR.RelatedOrgName',
    'TR.RICCode',
    'Primary Issue RIC',  #TR.PrimaryRIC
    'TR.PrimaryQuote',
    'TR.PrimaryRICCode',
    'TR.PrimaryInstrument',
    'TR.PrimaryIssueRICCode',
    'Ticker Symbol',  #TR.TickerSymbol
    'TR.TickerSymbolOld',
    'TR.TickerSymbolCode',
    'TR.IssuerTickerCode',
    'TR.WertCode',
    'TR.SEDOLCode',
    'CUSIP (extended)',  #TR.CUSIPExtended
    'TR.RegistrationNumber',
    'TR.InstrumentTypeCode',
    'TR.ISINCode',
    'TR.SEDOLCode',
    'TR.IssuerPICode',
    'TR.ExchangeMarketIdCode',
    'TR.ValorenCode',
    'TR.LipperRICCode',
    'TR.CommonCode',
    'TR.DUNSNUMBER',
    'TR.CIKNUMBER',
    'TR.InstrumentDescription',
    'TR.MemberIndexRic',
    # 'ORG ID',
    'TR.QuoteID',
    'TR.InstrumentID',
    'TR.MXID',
    'TR.EstimateId',
    'ESG Information Issuer Organization',
    'TR.UltimateParent',
    'TR.RelatedOrgId',
]
SUMMED_DATA: list[str] = [
    # Data that can't be put into categorical variables
    'Co. Business Summary',  #TR.CompanyInfoBusSummary
    'TR.BusinessSummary',
    'TR.CompanyInfoFinSummary',
    # and are not time series data or only averages of the last couple of weeks - seems to be all float types
    'TR.PricePctChgRelIdx5Y',
    'TR.PricePctChg5Y',
    'TR.PriceNetChg5Y',
    'TR.PriceAvgPctDiff90D',
    'TR.AvgMonthlyVolume13W',
    'TR.PriceAvgNetDiff2D',
    'TR.AvgDailyVolume80D',
    'TR.PriceAvgPctDiff40D',
    'TR.AvgDailyVolume3M',
    'TR.AvgDailyVolume90D',
    'TR.AvgDirMovIdxRating30D',
    'TR.PriceAvgNetDiff100D',
    'TR.AvgDailyValTraded30D',
    'TR.PriceAvgPctDiff240D',
    'TR.PriceAvgNetDiff50D',
    'TR.PriceAvg20D',
    'TR.PriceAvgPctDiff25D',
    'TR.PriceAvgPctDiff160D',
    'TR.AvgDailyVolume2D',
    'TR.AvgDailyVolume120D',
    'TR.PriceAvg10D',
    'TR.AvgDailyVolume25D',
    'TR.MovAvgIntersect60D200D',
    'TR.PriceAvgNetDiff10D',
    'TR.PriceAvgPctDiff250D',
    'TR.PriceAvgPctDiff180D',
    'TR.AvgDailyVolume150D',
    'TR.AvgDailyVolume5D',
    'TR.PriceAvgNetDiff30D',
    'TR.PriceAvgPctDiff200D',
    'TR.PriceAvgPctDiff20D',
    'TR.PriceAvg160D',
    'TR.PriceAvgPctDiff60D',
    'TR.PriceAvg40D',
    'TR.PriceAvg90D',
    'TR.AvgDailyVolume30D',
    'TR.PriceAvg25D',
    'TR.AvgDailyVolume10Day',
    'TR.AvgDailyVolume60D',
    'TR.PriceAvgPctDiff30D',
    'TR.PriceAvg180D',
    'TR.PriceAvgNetDiff25D',
    'TR.PriceAvgNetDiff180D',
    'TR.AvgDailyVolume6M',
    'TR.PriceAvgPctDiff10D',
    'TR.AvgDailyVolume180D',
    'TR.PriceAvg250D',
    'TR.AvgDailyVolume200D',
    'TR.PriceAvg6M',
    'TR.AvgDirMovIdxRating14D',
    'TR.AvgDailyVolume50D',
    'TR.PriceAvg2D',
    'TR.PriceAvgNetDiff200D',
    'TR.AvgDailyVolume160D',
    'TR.PriceAvgNetDiff240D',
    'TR.PriceAvgNetDiff250D',
    'TR.AvgDailyVolume5DPrior5D',
    'TR.PriceAvgNetDiff40D',
    'TR.PriceAvgNetDiff90D',
    'TR.AvgDailyVolume20D',
    'TR.PriceAvg100D',
    'TR.PriceAvg80D',
    'TR.AvgMonthlyVolume3M',
    'TR.PriceAvgPctDiff100D',
    'TR.PriceAvgNetDiff160D',
    'TR.PriceAvgPctDiff50D',
    'TR.PriceAvgNetDiff120D',
    'TR.PriceAvg240D',
    'TR.AvgDailyValTraded5D',
    'TR.PriceAvgPctDiff5D',
    'TR.AvgDailyVolume40D',
    'TR.PriceAvgNetDiff20D',
    'TR.MovAvgIntersect30D200D',
    'TR.PriceAvgNetDiff5D',
    'TR.PriceAvgNetDiff150D',
    'TR.AvgDailyValTraded52W',
    'TR.AvgDailyVolume250D',
    'TR.PriceAvg120D',
    'TR.MovAvgIntersect30D60D',
    'TR.AvgDailyVolume100D',
    'TR.PriceAvg3M',
    'TR.AvgDailyValTraded20D',
    'TR.PriceAvg30D',
    'TR.AvgDirMovIdxRating9D',
    'TR.AvgDailyVolume13W',
    'TR.PriceAvgPctDiff2D',
    'TR.AvgDailyVolume240D',
    'TR.PriceAvg60D',
    'TR.PriceAvgNetDiff60D',
    'TR.PriceAvg5D',
    'TR.PriceAvgPctDiff80D',
    'TR.PriceAvgNetDiff80D',
    'TR.PriceAvgPctDiff120D',
    'TR.PriceAvgPctDiff150D',
    'TR.MovAvgCDSignal',
    'TR.MovAvgCDLine2',
    'TR.MovAvgCDLine1',
    'TR.AvgMonthlyValTraded1Year',
    'TR.RelPricePctChg26W',
    'TR.VolumePctChg5D30D',
    'TR.PricePctChg90D',
    'TR.PriceNetChg1D',
    'TR.PricePctChg20D',
    'TR.PricePctChg8M',
    'TR.PricePctChg4M',
    'TR.PricePctChg11M',
    'TR.PriceNetChg5D',
    'TR.PriceNetChg3M',
    'TR.PriceNetChg30D',
    'TR.PricePctChg9M',
    'TR.PriceNetChg2M',
    'TR.VolumeNetChg5D30D',
    'TR.PricePctChg100D',
    'TR.PricePctChgYTD',
    'TR.RelPricePctChg1D',
    'TR.PriceNetChg8M',
    'TR.PriceNetChg20D',
    'TR.PriceNetChg240D',
    'TR.PricePctChg10M',
    'TR.PricePctChg4W',
    'TR.RelPricePctChg52W',
    'TR.PricePctChg6M',
    'TR.PricePctChg5D',
    'TR.RelPricePctChg13W',
    'TR.PricePctChg7M',
    'TR.PricePctChg120D',
    'TR.PricePctChgRelIdxYTD',
    'TR.PricePctChg26W',
    'TR.PriceNetChg90D',
    'TR.PricePctChg5M',
    'TR.PricePctChg40D',
    'TR.PricePctChg60D',
    'TR.PriceNetChg9M',
    'TR.PricePctChg25D',
    'TR.PricePctChg250D',
    'TR.PricePctChg50D',
    'TR.PricePctChgWTD',
    'TR.PricePctChg2D',
    'TR.PriceNetChg5M',
    'TR.PricePctChgRelIdx1Y',
    'TR.PricePctChgRelIdx5D',
    'TR.PricePctChg52W',
    'TR.PriceNetChg250D',
    'TR.PriceNetChg10D',
    'TR.PriceNetChg50D',
    'TR.PricePctChg52WkHigh',
    'TR.PricePctChg1D',
    'TR.RelPricePctChg4W',
    'TR.PricePctChg80D',
    'TR.PriceNetChg80D',
    'TR.PriceNetChg10M',
    'TR.PriceNetChg100D',
    'TR.PricePctChg1Y',
    'TR.PricePctChgRelIdx3M',
    'TR.PriceNetChg120D',
    'TR.PriceNetChg1M',
    'TR.PriceNetChg200D',
    'TR.PriceNetChg150D',
    'TR.PriceNetChg7M',
    'TR.PricePctChg2M',
    'TR.PriceNetChg4M',
    'TR.PriceNetChg40D',
    'TR.PricePctChgRelIdx1M',
    'TR.PricePctChg150D',
    'TR.PricePctChgQTD',
    'TR.PricePctChg3M',
    'TR.PricePctChg13W',
    'TR.PricePctChg200D',
    'TR.PricePctChg30D',
    'TR.PriceNetChg1Y',
    'TR.PriceNetChg25D',
    'TR.PriceRelSMAPctChg200D',
    'TR.PriceNetChg2D',
    'TR.PriceNetChg180D',
    'TR.PricePctChg10D',
    'TR.PricePctChgMTD',
    'TR.PriceNetChg11M',
    'TR.PricePctChg180D',
    'TR.VolumePctChg1D',
    'TR.PricePctChg1M',
    'TR.PricePctChg52WkLow',
    'TR.PriceNetChg160D',
    'TR.PricePctChg160D',
    'TR.PriceNetChg6M',
    'TR.RelPricePctChgYTD',
    'TR.PricePctChg240D',
    'TR.PriceNetChg60D',
    'TR.AlphaMthlyUp5Y',
    'TR.SharpeRatioWklyDown3Y',
    'TR.BetaFiveYear',
    'TR.BetaWklyUp3Y',
    'TR.SharpeRatioWklyUp3Y',
    'TR.SharpeRatioWklyDown2Y',
    'TR.BetaWklyAdj2Y',
    'TR.SharpeRatioWkly3Y',
    'TR.AlphaWklyUp3Y',
    'TR.AlphaWklyUp2Y',
    'TR.BetaWklyUp2Y',
    'TR.AlphaMthly5Y',
    'TR.SharpeRatioWkly2Y',
    'TR.AlphaWkly3Y',
    'TR.BetaWkly3Y',
    'TR.AlphaWklyDown3Y',
    'TR.AlphaWklyDown2Y',
    'TR.SharpeRatioMthlyUp5Y',
    'TR.BetaWklyDown2Y',
    'TR.BetaFiveYearAdj',
    'TR.AlphaWkly2Y',
    'TR.SharpeRatioMthly5Y',
    'TR.SharpeRatioMthlyDown5Y',
    'TR.BetaWklyAdj3Y',
    'TR.SharpeRatioWklyUp2Y',
    'TR.AlphaMthlyDown5Y',
    'TR.BetaWkly2Y',
    'TR.BetaWklyDown3Y',
    'Average Daily Value Traded – 1 Week',
    'TR.Price52WeekHigh',
    'TR.Price52WkHighFlg5D',
    'TR.Price52WeekLow',
    'TR.High1W',
    'TR.RSIWilder3D',
    'TR.RSIWilder14D',
    'TR.Low1W',
    'TR.RSIWilder30D',
    'TR.SortinoRatio156W',
    'TR.Price52WkLowFlg1D',
    'TR.Volume3WSum',
    'TR.Price52WkHighFlg1D',
    'TR.RSIWilder9D',
    '1 Week Total Return Cross Asset',
    'TR.Price52WkLowFlg5D',
    'Average Daily Value Traded – 3 Months',
    'TR.RSISimple30D',
    'TR.VolatilityCloseToClose120D',
    'TR.Volatility100D',
    'TR.Price200DayAverage',
    'TR.Price150DayAverage',
    'TR.Volatility260D',
    'TR.DirMovIdxDiPlus',
    'TR.PriceDeviation',
    'TR.Volatility120D',
    'TR.Volatility25D',
    'TR.Volatility80D',
    'TR.Price50DayAverage',
    'TR.VolumeSum10D',
    'TR.VolumeBlockSum10D',
    'Average Daily Value Traded – 6 Months',
    'TR.RSIExp14D',
    'TR.VolumeDeviation',
    'TR.Volatility250D',
    'TR.VolumeNonBlockSum10D',
    'TR.Volatility150D',
    'TR.Volatility160D',
    'TR.RSISimple14D',
    'TR.RSISimple9D',
    'TR.Volatility5D',
    'TR.Volatility2D',
    'TR.BetaDaily180D',
    'TR.RSIExp30D',
    'TR.VolatilityCloseToClose90D',
    'TR.Volatility40D',
    'TR.Volatility200D',
    'TR.Volatility20D',
    'TR.OrganizationID',
    'TR.Liquidity10DAmt',
    'TR.Volatility180D',
    'TR.Volatility240D',
    'TR.RSISimple3D',
    'TR.RSIExp9D',
    'TR.VolatilityCloseToClose20D',
    'TR.BetaDaily90D',
    'TR.RSIExp3D',
    'Average Daily Value Traded – 2 Months',
    'TR.Volatility60D',
    'TR.Volatility10D',
    'TR.VolatilityCloseToClose60D',
    'TR.Volatility50D',
    'TR.BetaDown',
    'TR.Volatility90D',
    'TR.DirMovIdxDiMinus',
    'TR.Liquidity10DVol',
    'TR.Volatility30D',
    'TR.SortinoRatio60M',
    'TR.High1M',
    'TR.Low1M',
    '3-year Price PCT Change',
    'TR.BollingerLowBand',
    'TR.BollingerMidBand',
    'TR.BollingerUpBand',
    'TR.BetaUp',
    'TR.MoneyFlowTotalVol',
    'TR.MoneyFlowNonBlockVol',
    'TR.StopAndReversalPoint',
    'TR.SettlementPeriod',
]
TOO_PRECISE_DATA: list[str] = [
    # Data that is too specific or is too distinctive and has too much variance
    'TR.CmnLegalAddressLine1',
    'TR.CommonHQAddressLine1',
    'TR.CommonHQAddressLine2',
    'TR.CommonHQAddressLine3',
    'TR.LegalAddressLine1',
    'TR.LegalAddressLine2',
    'TR.HQAddressLine1',
]
REDUNDANT_DATA: list[str] = [
    # Redundant information
    'TR.ExchangeCode',  #instead, use TR.HQCountryCode
    'TR.RegCountryCode',  #instead, use TR.HQCountryCode
    'TR.RegStateProvince',  #instead, use TR.HQStateProvince
    'TR.HQAddressStateProvince',  #instead, use TR.HQStateProvince
    'TR.ExchangeRegion',  #instead, use TR.HQCountryCode
    'TR.HQAddressCity',  #instead, use TR.HeadquartersCity
    'TR.HeadquartersRegion',  #instead, use TR.HQCountryCode
    'TR.LegalAddressCountryISO',  #instead, use TR.HQCountryCode
    'TR.HQAddressCountryISO',  #instead, use TR.HQCountryCode
    'TR.LegalAddressPostalCode',  #instead, use TR.HQAddressPostalCode
    'TR.ImmediateParentCountryHQ',  #instead, use TR.ImmediateParentISOCountryHQ
    'TR.UltimateParentCountryHQ',  #instead, use TR.HQCountryCode
    'TR.UltimateParentISOCountryHQ',  #instead, use TR.HQCountryCode
    'TR.RelatedOrgCountry',  #instead, use TR.HQCountryCode
    'TR.HeadquartersCountry',  #instead, use TR.HQCountryCode
    'TR.NAICSSectorAll',  #instead, use TR.NAICSSector
    'TR.NAICSIndustryGroupAll',  #instead, use TR.NAICSIndustryGroup
    'TR.NAICSNationalIndustryAll',  #instead, use TR.NAICSNationalIndustry
    'TR.NAICSIndustryGroupAllCode',  #instead, use TR.NAICSIndustryGroupCode
    'TR.NAICSNationalIndustryAllCode',  #instead, use TR.NAICSNationalIndustryCode
    'TR.NAICSSubsectorAll',  #instead, use TR.NAICSSubsector
    'TR.NAICSNationalIndustry',  #instead, use TR.NAICSInternationalIndustry
    'TR.NAICSInternationalIndustryAll',  #instead, use TR.NAICSInternationalIndustry
    'TR.NAICSInternationalIndustry',  #instead, use TR.NAICSIndustryGroup
    'TR.NAICSInternationalIndustryAllCode',  #instead, use TR.NAICSInternationalIndustryCode
    'TR.NAICSSubsectorAllCode',  #instead, use TR.NAICSSubsectorCode
    'TR.QuoteMarketCapitalization',  #instead, use TR.QuoteMarketCap
    'TR.UltimateParentId',  #instead, use TR.UltimateParent
    'TR.PriceMoPriceCurrency',  #instead, use Currency Code
    'TR.InstrumentListingStatus', #instead, use TR.InstrumentIsActive
    'TR.OrganizationTypeCode', #instead, use TR.OrganizationType
]
DATES: list[str] = [
    # Dates of reporting
    'Number Employees Date',  #TR.CompanyNumEmployDate
    'Number of Holders Date',  #TR.CompanyShldrNumDate
    'Shares O/S Date',  #TR.CompanySharesOutDate
    'Fiscal Year End Date',  #TR.CompanyFYearEnd
    'Source Filing Date',  #TR.CompanySrcFileDate
    'TR.CompanyPublicSinceDate',
    'TR.Price52WeekLowDate',
    'TR.DataThroughDate',
    'TR.FirstTradeDate',
    'TR.MemberIndexDate',
    'TR.CompanyIncorpDate',
    'TR.DataThroughDateValuation',
    'TR.IPODate',
    'TR.LowDate1W',
    'TR.HighDate1W',
    'TR.LowDate1M',
    'TR.HighDate1M',
    'TR.RetireDate',
    'TR.OrgFoundedDay',
    'TR.Price52WeekHighDate',
    'TR.SegmentRevenueProdNoteDate',
    'TR.SegmentEBITDAEstDate',
    'TR.SegmentEBITEstDate',
    'TR.SegmentEBITDAProdNoteDate',
    'TR.SegmentEBITEstConfirmDate',
    'TR.SegmentEBITEstStopDate',
    'TR.SegmentRevenueEstDate',
    'TR.SegmentRevenueEstConfirmDate',
    'TR.SegmentOpProfitEstDate',
    'TR.SegmentOpProfitProdNoteDate',
    'TR.SegmentEBITProdNoteDate',
    'TR.SegmentOpProfitEstConfirmDate',
    'TR.SegmentEBITDAEstConfirmDate',
    'TR.SegmentOrganicSalesGrowthEstConfirmDate',
    'TR.SegmentOrganicSalesGrowthEstDate',
    'TR.SegmentOrganicSalesGrowthProdNoteDate',
    'TR.SegmentNumofStoresByTotalProdNoteDate',
    'TR.SegmentNumofStoresByTotalEstConfirmDate',
    'TR.SegmentNumofStoresByTotalEstDate',
    'TR.SegmentSubscribersEstConfirmDate',
    'TR.SegmentSubscribersEstDate',
    'TR.SegmentSubscribersProdNoteDate',
    'TR.SegmentNetSubscriberAddsEstDate',
    'TR.SegmentNetSubscriberAddsProdNoteDate',
    'TR.SegmentNetSubscriberAddsEstConfirmDate',
    'TR.SegmentDailyActiveUsersEstConfirmDate',
    'TR.SegmentDailyActiveUsersProdNoteDate',
    'TR.SegmentDailyActiveUsersEstDate',
    'TR.SegmentNumofStoresOpenedByTotalProdNoteDate',
    'TR.SegmentRevenueEstStopDate',
    'TR.SegmentEBITDAReportedProdNoteDate',
    'TR.SegmentNumofStoresOpenedByTotalEstDate',
    'TR.SegmentEBITDAEstStopDate',
    'TR.SegmentNumofStoresClosedProdNoteDate',
    'TR.SegmentNumofStoresClosedEstDate',
    'TR.SegmentNumofStoresOpenedByTotalEstConfirmDate',
]
OTHER_DATA: list[str] = [
    # Other
    'TR.SegmentRevenueEstBrokerName',  #The name of the broker forecasting the estimate.
    'TR.MICName',  #MIC name
    'TR.TaxAuthorityName',  #The authority to which the respective Organization pays taxes.
    'TR.PriceMainIndexRIC',
    #The main Index RIC applicable to the entity in question and used as the benchmark in values like Dividend Yield Relative to Primary Index.
    'TR.OrgProviderTypeCode',  #Organization Provider type code.
    'TR.CUSIPCode',
    'TR.CinCUSIPCode',
    #Committee on Uniform Securities Identification Procedures Identifier for Non US & Canadian companies.
    'TR.ExchangeCountry',  #Country where the instrument trades.
    'TR.ExchangeCountryCode',  #ISO2 country code where the instrument trades.
    'TR.OrganizationStatusCode',  #Indicates whether the Organization is active in the real world.
    'TR.EquityLocalCode',  #Local code
    'TR.AssetIDCode',  #The Refinitiv Fixed Income identifier.
    'TR.TaxAuthority',  # I don't think that to know where the taxes are paid is relevant.
    'TR.TaxAuthorityName',
    'TR.OrganizationVerified',  # only same values
    'TR.IsRule144aRegistered',  # only same values
    'TR.IsCompositeQuote',  # only same values
    'TR.IsPrimaryQuote',  # don't know what this is and only 5 are not
    'TR.OrgFoundedMonth',
    'TR.IsPrimaryInstrument',  # only three are not
    'TR.PrimListFunExist',  # only same values
    'TR.HasFundamentalCoverage',  # only same values
    'TR.HasESGCoverage'  # only same values
]
# unsure about these codes and classifiers
UNSURE = [
    'TR.OrgSubtypeCode',
    'TR.TRBCBusinessSectorAllCode',
    'TR.ICBSectorCode',
    'TR.TRBCActivityAllCode',
    'TR.ICBSupersectorCode',
    'TR.TRBCEconSectorAllCode',
    'TR.ICBIndustryCode',
    'TR.TRBCIndustryCode',
    'TR.TRBCActivityCode',
    'TR.TRBCEconSectorCode',
    'TR.TRBCIndustryGroupAllCode',
    'TR.InstrumentListingStatusCode',
    'TR.TRBCIndustryGroupCode',
    'TR.ICBSubsectorCode',
    'TR.TRBCIndustryAllCode',
    'TR.OrgTypeCode',
    'TR.AssetCategoryCode',
    'TR.TRBCBusinessSectorCode',
    'TR.SICIndustryCode',
    'TR.SICDivisionCode',
    'TR.SICIndustryGroupCode',
    'TR.SICMajorGroupCode',
    'TR.CriticalCountry1',
    'TR.IsCountryPrimaryQuote',
    'TR.ImmediateParentISOCountryHQ',
    'TR.ImmediateParent',  # has around 200 values that have another parent company / could encode company=parent to 0
    'TR.NACEClassification',  # is the standard European industry classification system
    'TR.IsDelistedQuote',  # flag if a quote is delisted (only 1% are)
    'TR.QuoteMarketCap', # represents the marked value - Has to be reloaded with USD as currency,
    'TR.NAICSIndustryGroup',  #focus on GICSCodes
    'TR.NAICSIndustryGroupCode',  #TR.NAICSSectorCode is more generalized
    'TR.NAICSNationalIndustryCode',  #TR.NAICSSectorCode is more generalized
    'TR.NAICSInternationalIndustryCode',  #TR.NAICSSectorCode is more generalized
    'TR.NAICSSubsectorCode',  #TR.NAICSSectorCode is more generalized
    'TR.NAICSSectorAllCode',  #TR.NAICSSectorCode is more generalized
    'TR.CompanyIncorpRegion', #focus on TR.HQCountryCode
    'TR.NAICSSector',  #focus on GICSCodes
    'TR.NAICSSectorCode',  #focus on GICSCodes
    'TR.NAICSSubsector',  #focus on GICSCodes
    'TR.TRBCBusinessSectorAll',  #focus on GICSCodes
    'TR.TRBCIndustryGroupAll',  #focus on GICSCodes
    'TR.TRBCBusinessSector',  #focus on GICSCodes
    'TR.TRBCEconomicSector',  #focus on GICSCodes
    'TR.TRBCEconSectorAll',  #focus on GICSCodes
    'TR.TRBCIndustryGroup',  #focus on GICSCodes
    'TR.TRBCActivityAll',  #focus on GICSCodes
    'TR.TRBCIndustryAll',  #focus on GICSCodes
    'TR.TRBCIndustry',  #focus on GICSCodes
    'TR.TRBCActivity',  #focus on GICSCodes
    'TR.ICBSupersector',  #focus on GICSCodes
    'TR.ICBSubsector',  #focus on GICSCodes
    'TR.ICBIndustry',  #focus on GICSCodes
    'TR.ICBSector',  #focus on GICSCodes
    'TR.GICSIndustry', #focus on GICSSectorCodes
    'TR.GICSIndustryCode', #focus on GICSSectorCodes
    'TR.GICSIndustryGroup', #focus on GICSSectorCodes
    'TR.GICSIndustryGroupCode', #focus on GICSSectorCodes
    'TR.GICSSector', #focus on GICSSectorCodes
    'TR.GICSSubIndustry', #focus on GICSSectorCodes
    'TR.GICSSubIndustryCode', #focus on GICSSectorCodes
    'TR.HQAddressPostalCode' #focus on HeadquartersCity
]