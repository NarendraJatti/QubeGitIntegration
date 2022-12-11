import json 
import pandas as pd 
from pandas.io.json import json_normalize
import psycopg2
import json

with open("C:\\Users\\ehpov\\Materials_0c3c0b4d-68ff-4c58-a97a-0ba3e1eea681_20220930095106.json","r") as f:
    dt = json.load(f)
    # print(data)

generaldata = pd.json_normalize(dt['materials'])
generaldata = generaldata.drop(columns=['name','classifications','sales','materialDescriptions','unitsOfMeasure',
'registrations','internationalArticles','generalData.markedForDeletion','generalData.temperatureConditions','environment.highlyViscous','environment.inBulkLiquid',
'environment.dangerousGoods','environment.unpackedGoodsNumber','environment.referencePackagingMaterial',
'bayerData.general.globalCluster','bayerData.general.lifeCyclePhase','bayerData.general.successorMaterial',
'bayerData.reference.realSubstanceNumber','bayerData.reference.registrationVersion','bayerData.segmentation.thirdPartyActiveIngredient','bayerData.segmentation.productSegmentation','bayerData.segmentation.totalActiveIngredients',
'bayerData.productLifeCycle.gotoMarketDate','bayerData.productLifeCycle.lastProducedDate','bayerData.productLifeCycle.lastSalesDate',
'bayerData.authorizationGroupPmd','created.clientId','created.userId','created.time','updated.clientId','updated.userId','updated.time',
'deleted.clientId','deleted.userId','deleted.time'], axis=1)

generaldata = generaldata.drop_duplicates()

generaldata.rename(columns = {'generalData.baseUnitOfMeasure':'baseUnitOfMeasure',
'generalData.materialGroup':'materialGroup','generalData.materialGroupDescription':'materialGroupDescription',
'generalData.materialGroupDescriptionLong':'materialGroupDescriptionLong','generalData.materialType':'materialType','generalData.materialProductHierarchy':'materialProductHierarchy',
'generalData.materialRecordCreateDate':'materialRecordCreateDate','generalData.materialRecordChangedDate':'materialRecordChangedDate',
'generalData.division':'division','generalData.traitCode':'traitCode','generalData.crossPlantMaterialStatus':'crossPlantMaterialStatus',
'generalData.industryStandardDescription':'industryStandardDescription','generalData.productionInspectionMemo':'productionInspectionMemo',
'generalData.deleted':'deleted','generalData.productGroup':'productGroup','generalData.productHierarchyLevel1':'productHierarchyLevel1',
'generalData.productHierarchyLevel1Text':'productHierarchyLevel1Text',
'generalData.productHierarchyLevel2':'productHierarchyLevel2','generalData.productHierarchyLevel2Text':'productHierarchyLevel2Text',
'generalData.productHierarchyLevel3':'productHierarchyLevel3','generalData.productHierarchyLevel3Text':'productHierarchyLevel3Text',
'generalData.productHierarchyLevel4':'productHierarchyLevel4','generalData.productHierarchyLevel4Text':'productHierarchyLevel4Text',
'generalData.productHierarchyLevel5':'productHierarchyLevel5','generalData.productHierarchyLevel5Text':'productHierarchyLevel5Text',
'generalData.productHierarchyLevel6':'productHierarchyLevel6','generalData.productHierarchyLevel6Text':'productHierarchyLevel6Text',
'dimensions.weightUnit':'weightUnit','dimensions.weightNet':'weightNet','dimensions.weightGross':'weightGross',
'dimensions.volume':'volume','dimensions.volumeUnit':'volumeUnit','dimensions.sizeDimensions':'sizeDimensions',
'bayerData.packing.packagingCode':'packagingCode','bayerData.packing.quantityPerShippingUnit':'quantityPerShippingUnit',
'bayerData.packing.primaryPackagingMaterial':'primaryPackagingMaterial',
'bayerData.packing.unitOfMeasureForPrimaryPackaging':'unitOfMeasureForPrimaryPackaging',
'bayerData.packing.uNCoding':'uNCoding','bayerData.packing.stackingFactor':'stackingFactor',
'attributes.commercialName':'commercialName','attributes.cropName':'cropName','attributes.cropCode':'cropCode',
'attributes.brandName':'brandName','attributes.brandCode':'brandCode','attributes.manufacturingName':'manufacturingName',
'attributes.manufacturingTraitVersion':'manufacturingTraitVersion','attributes.lexiconProductPubkey':'lexiconProductPubkey',
'attributes.businessGroup':'businessGroup','attributes.lineOfBusiness':'lineOfBusiness',
'attributes.productName':'productName'}, inplace = True)

generaldata_p08 = generaldata[(generaldata.source == '08') & (generaldata.materialType == 'FERT') & 
(~(generaldata['materialGroup']).isin(['F0000014','F0000015','70141702','GS010','GI010','GU010','GFP50','GF010','GT010','GCP50']))]

generaldata_pbc = generaldata[(generaldata.source == 'BC') & (generaldata['crossPlantMaterialStatus'].isin(['4','7','8','9'])) &
(generaldata['materialType'].isin(['YBIO','YK','YV'])) &
(generaldata.baseUnitOfMeasure != 'ST') & 
(~(generaldata['productHierarchyLevel5']).isin(['5800','0128']))]

merge_pbc_p08 = pd.concat([generaldata_p08,generaldata_pbc])
merge_pbc_p08 = merge_pbc_p08.drop_duplicates()

#classifications-characteristics

cls = pd.json_normalize(dt['materials'],record_path='classifications',
meta=['materialId','materialNumber','source'])

cls1 = pd.json_normalize(data=dt['materials'],record_path=['classifications','characteristics'],
meta=['materialId','materialNumber','source'])

merge_cls = cls.merge(cls1,how='outer',on=('materialId','materialNumber','source'))
merge_cls = merge_cls.drop(columns=['characteristics','longTexts'], axis=1)
merge_cls = merge_cls.drop_duplicates()
merge_cls.rename(columns = {'name_x':'clsfic_name','name_y':'clsfic_char_name','type':'clsfic_type',
'status':'clsfic_status','value':'clsfic_char_value'}, inplace = True)

#merge generaldata and classifications
merge_cls_gen = merge_pbc_p08.merge(merge_cls,how='outer',on=('materialId','materialNumber','source'))
merge_cls_gen = merge_cls_gen.drop_duplicates()

#sales-distributionChains-salesTexts

sales = pd.json_normalize(dt['materials'],record_path='sales',
meta=['materialId','materialNumber','source'])
sales = sales.drop(columns=['distributionChains'], axis=1)
sales = sales.drop_duplicates()

sales1 = pd.json_normalize(data=dt['materials'],record_path=['sales','distributionChains'],
meta=['materialId','materialNumber','source'])
sales1 = sales1.drop(columns=['materialGroupOne','materialGroupTwo','materialGroupThree','materialGroupFour','deleted','salesTexts'], axis=1)
sales1 = sales1.drop_duplicates()
sales1 = sales1[(sales1.materialGroupFive != '')]
merge_sales1 = sales.merge(sales1,how='outer',on=('materialId','materialNumber','source'))
merge_sales1 = merge_sales1.drop_duplicates()

sales2 = pd.json_normalize(data=dt['materials'],record_path=['sales','distributionChains','salesTexts'],
meta=['materialId','materialNumber','source'])
sales2 = sales2.drop(columns=['lineNumber'], axis=1)
sales2 = sales2.drop_duplicates()

merge_sales = merge_sales1.merge(sales2,how='outer',on=('materialId','materialNumber','source'))
merge_sales = merge_sales.drop_duplicates()
merge_sales = merge_sales[((merge_sales['salesOrg']).isin(['IN01','IN07','IN09','IN81'])) & ((merge_sales['distributionChannel']).isin(['91','84','27','50','80']))]
merge_sales.rename(columns = {'text':'sales_text','language':'sales_language'}, inplace = True)

#merge classification & sales
merge_cls_sls = merge_cls_gen.merge(merge_sales,how='outer',on=('materialId','materialNumber','source'))
merge_cls_sls = merge_cls_sls.drop_duplicates()

#material Descriptions
desc = pd.json_normalize(data=dt['materials'],record_path='materialDescriptions',
meta=['materialId','materialNumber','source'])
desc = desc.drop_duplicates()

#merge classification & sales & material descriptions
merge_mdesc = merge_cls_sls.merge(desc,how='outer',on=('materialId','materialNumber','source'))
merge_mdesc = merge_mdesc.drop_duplicates()
merge_mdesc = merge_mdesc[merge_mdesc.language == 'EN']
merge_mdesc.rename(columns = {'description_x':'clsfic_char_desc','description_y':'description','language':'materiallanguage'}, inplace = True)

#Unit of Measure
uom = pd.json_normalize(data=dt['materials'],record_path='unitsOfMeasure',
meta=['materialId','materialNumber','source'])
uom = uom.drop(columns=['lowerLevelUnitOfMeasure','sortNumber','leadingBatchUnitOfMeasure',
'batchSpecificValuationInd','unitOfMeasurementOfCharacteristic','unitsOfMeasureUsage',
'genericMaterialWithLogisticalVariants','globalTradeItemNumberVariant','volumeAfterNesting',
'maxStackFactor','capacityUsage','dimensions.length','dimensions.width','dimensions.height',
'dimensions.unitOfDimension','dimensions.displayUnitOfDimension'], axis=1)
uom = uom.drop_duplicates()

#merge classification & sales & material descriptions & UOM
merge_final = merge_mdesc.merge(uom,how='outer',on=('materialId','materialNumber','source'))
merge_final = merge_final.drop_duplicates()
merge_final.rename(columns = {'eanInfo.internationalArticleNumber':'eanInfo_internationalArticleNumber',
'eanInfo.articleNumberCategory':'eanInfo_articleNumberCategory','volume.volume':'uom_volume','volume.volumeUnit':'uom_volumeunit',
'volume.displayVolumeUnit':'uom_displayvolumeunit','weight.grossWeight':'uom_grossweight',
'weight.weightUnit':'uom_weightunit','weight.displayWeightUnit':'uom_displayweightunit'}, inplace = True)

merge_final.to_csv("C:\\Users\\ehpov\\outpt\\merge_final.csv",index=False)

conn_string = "host='dk-insights-db1.crz2cfrftsts.us-east-1.rds.amazonaws.com' dbname='dkinsights_db' user='root' password='mypassword12345' port='5432'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

cursor.execute('TRUNCATE TABLE material_poc.in_stg_material_master;')

load_file = open('C:\\Users\\ehpov\\outpt\\merge_final.csv')
sql_cmd = """COPY material_poc.in_stg_material_master FROM STDIN WITH 
CSV 
HEADER 
DELIMITER AS ',' 
"""
cursor.copy_expert(sql=sql_cmd,file=load_file)

conn.commit()
cursor.close()
conn.close()