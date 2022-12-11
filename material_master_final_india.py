import json
import pandas as pd
from pandas.io.json import json_normalize
import psycopg2, json, os
from datetime import datetime

cmd = 'aws s3 sync s3://dk-insights/Input/Assets/Productvtwo/India/Materials/ /home/ubuntu/material/input'
os.system(cmd)

path = '/home/ubuntu/material/input'
os.chdir(path)
print('Job started')
print(datetime.now())
for jfile in os.listdir():
    if jfile.endswith(".json"):
        file_path = f"{path}/{jfile}"
        st = datetime.now()
        print(file_path)

        with open(file_path,"r") as f:
            dt = json.load(f)

            generaldata = pd.json_normalize(dt['materials'])
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
            'dimensions.weightUnit':'dim_weightUnit','dimensions.weightNet':'dim_weightNet','dimensions.weightGross':'dim_weightGross',
            'dimensions.volume':'dim_volume','dimensions.volumeUnit':'dim_volumeUnit','dimensions.sizeDimensions':'dim_sizeDimensions',
            'bayerData.general.globalCluster':'globalcluster','bayerData.general.lifeCyclePhase':'lifecyclephase',
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

            #classifications-characteristics
            cls = pd.json_normalize(dt['materials'],record_path='classifications',
            meta=['materialId','materialNumber','source'])

            cls1 = pd.json_normalize(data=dt['materials'],record_path=['classifications','characteristics'],
            meta=['materialId','materialNumber','source'])

            merge_cls = cls.merge(cls1,how='left',on=('materialId','materialNumber','source'))
            merge_cls.rename(columns = {'name_x':'clsfic_name','name_y':'clsfic_char_name','type':'clsfic_type',
            'status':'clsfic_status','value':'clsfic_char_value','description':'clsfic_char_desc'},  inplace = True)

            #merge generaldata and classifications
            merge_cls_gen = merge_pbc_p08.merge(merge_cls,how='left',on=('materialId','materialNumber','source'))
            merge_cls_gen = merge_cls_gen[(merge_cls_gen.clsfic_name != '') & (merge_cls_gen.clsfic_char_name != '')
            & (merge_cls_gen.clsfic_type != '') & (merge_cls_gen.clsfic_status != '') & (merge_cls_gen.clsfic_char_desc != '')]

            #sales-distributionChains-salesTexts
            sales = pd.json_normalize(dt['materials'],record_path='sales',
            meta=['materialId','materialNumber','source'])
            sales = sales.drop(columns=['distributionChains'], axis=1)

            merge_sales = pd.DataFrame()
            sales2 = pd.DataFrame()

            sales1 = pd.json_normalize(data=dt['materials'],record_path=['sales','distributionChains'],
            meta=['materialId','materialNumber','source'])

            sales1 = sales.merge(sales1,how='left',on=('materialId','materialNumber','source'))
            sales1_df = pd.DataFrame(sales1)

            if 'salesTexts' in sales1.columns :
                sales1["text"] = ""
                sales1["language"] = ""
                sales1 = sales1.assign(text = "",language = "")
                sales1 = sales1.drop(columns=['materialGroupOne','materialGroupTwo','materialGroupThree','materialGroupFour','deleted','salesTexts'], axis=1)

            if 'salesTexts' not in sales1.columns :
                sales2 = pd.json_normalize(data=dt['materials'],record_path=['sales','distributionChains','salesTexts'],
                meta=['materialId','materialNumber','source'])

            merge_sales = pd.concat([sales1,sales2])
            merge_sales = merge_sales[(merge_sales.materialGroupFive != '') &
            ((merge_sales['salesOrg']).isin(['IN01','IN07','IN09','IN81'])) &
            ((merge_sales['distributionChannel']).isin(['91','84','27','50','80']))]
            #& (merge_sales.salesOrg != '')]

            #merge classification & sales
            merge_cls_sls = merge_cls_gen.merge(merge_sales,how='left',on=('materialId','materialNumber','source'))
            merge_cls_sls.rename(columns = {'text':'sales_text','language':'sales_language',
            'unitOfMeasure':'sales_uom'}, inplace = True)

            #material Descriptions
            desc = pd.json_normalize(data=dt['materials'],record_path='materialDescriptions',
            meta=['materialId','materialNumber','source'])

            #merge classification & sales & material descriptions
            merge_mdesc = merge_cls_sls.merge(desc,how='left',on=('materialId','materialNumber','source'))
            merge_mdesc = merge_mdesc[merge_mdesc.language == 'EN']
            #merge_mdesc.rename(columns = {'description_x':'clsfic_char_desc','description_y':'description','language':'materiallanguage'}, inplace = True)
            merge_mdesc.rename(columns = {'description':'material_desc','language':'material_language'}, inplace = True)
            #Unit of Measure
            uom = pd.json_normalize(data=dt['materials'],record_path='unitsOfMeasure',
            meta=['materialId','materialNumber','source'])

            #merge classification & sales & material descriptions & UOM
            merge_final = merge_mdesc.merge(uom,how='left',on=('materialId','materialNumber','source'))
            merge_final.rename(columns = {'eanInfo.internationalArticleNumber':'eanInfo_internationalArticleNumber',
            'eanInfo.articleNumberCategory':'eanInfo_articleNumberCategory','volume.volume':'uom_volume','volume.volumeUnit':'uom_volumeunit',
            'volume.displayVolumeUnit':'uom_displayvolumeunit','weight.grossWeight':'grossweight',
            'weight.weightUnit':'weightunit','weight.displayWeightUnit':'displayweightunit'}, inplace = True)

            final = pd.DataFrame(merge_final)
            final = final[['materialId','materialNumber','source','baseUnitOfMeasure','materialGroup','materialGroupDescription','materialGroupDescriptionLong','materialType',
            'materialProductHierarchy','materialRecordCreateDate','materialRecordChangedDate','division','traitCode','crossPlantMaterialStatus',
            'industryStandardDescription','productionInspectionMemo','deleted','productGroup','productHierarchyLevel1','productHierarchyLevel1Text',
            'productHierarchyLevel2','productHierarchyLevel2Text','productHierarchyLevel3','productHierarchyLevel3Text','productHierarchyLevel4',
            'productHierarchyLevel4Text','productHierarchyLevel5','productHierarchyLevel5Text','productHierarchyLevel6','productHierarchyLevel6Text',
            'dim_weightUnit','dim_weightNet','dim_weightGross','dim_volume','dim_volumeUnit','dim_sizeDimensions','globalcluster','lifecyclephase','packagingCode',
            'quantityPerShippingUnit','primaryPackagingMaterial','unitOfMeasureForPrimaryPackaging','uNCoding','stackingFactor','commercialName','cropName',
            'cropCode','brandName','brandCode','manufacturingName','manufacturingTraitVersion','lexiconProductPubkey','businessGroup','lineOfBusiness',
            'productName','clsfic_name','clsfic_type','clsfic_status','clsfic_char_name','clsfic_char_value','clsfic_char_desc','salesOrg','distributionChannel',
            'materialGroupFive','sales_text','sales_language','sales_uom',
            'material_desc','material_language','alternateUnitOfMeasure','conversionFactor',
            'category','displayUnitOfMeasure','eanInfo_internationalArticleNumber','eanInfo_articleNumberCategory','uom_volume','uom_volumeunit',
            'uom_displayvolumeunit','grossweight','weightunit','displayweightunit']]

            final = final[((final['clsfic_name']).isin(['YCS_SMT','YCS_BRAND_REPORT','Z09_MATERIAL_IN','SG_CORN','CROP_CHEM_PRODUCTS'])) &
            ((final['clsfic_char_name']).isin(['YCS_BRAND','YCS_SMT_BRAND_NAME','Z09_IN_MATERIAL_GROUP','SG_PACKAGESIZE','SG_BRAND',
            'SG_ACRONYMNAME','CROP_PRIMARY_PACKAGE_CONTENTS','CROP_PRODUCT_NAME','SG_PACKAGETYPE',
            'CROP_PRIMARY_PACKAGE_TYPE']))]

            final["businessownerapproved"] = ""
            final["approvedformanualexclusion"] = ""
            final["hybridvalue"] = ""
            final["filename"] = jfile
            final["dkin_countrycode"] = "IN"
            final = final.assign(businessownerapproved = "", approvedformanualexclusion = "",
            hybridvalue = "", filename = jfile, dkin_countrycode = "IN")

            final.drop_duplicates()

            load_file_nm = '/home/ubuntu/material/csv/'+'tmp_material_master.csv'
            final.to_csv(load_file_nm,index=False)

            conn_string = "host='dk-insights-db.crz2cfrftsts.us-east-1.rds.amazonaws.com' dbname='dkinsights_db' user='root' password='mypassword12345' port='5432'"
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()

            cursor.execute('TRUNCATE TABLE material_poc.in_stg_material_master;')

            load_file = open(load_file_nm)
            sql_cmd = """COPY material_poc.in_stg_material_master FROM STDIN WITH
            CSV
            HEADER
            DELIMITER AS ','
            """
            cursor.copy_expert(sql=sql_cmd,file=load_file)

            sql_cmd2 = """INSERT INTO material_poc."MD_DKINS_MATERIAL_TGT"
            (materialid, "Materialnumber", "source", "Materialdescription", baseunitofmeasure, materialgroup, materialgroupdescription,
            materialgroupdescriptionlong, materialtype, materialproducthierarchy, materialrecordcreatedate, materialrecordchangeddate,
            traitcode, crossplantmaterialstatus, industrystandarddescription, productioninspectionmemo, deleted, productgroup, producthierarchylevel1, producthierarchylevel1text, producthierarchylevel2, producthierarchylevel2text, producthierarchylevel3, producthierarchylevel3text, producthierarchylevel4, producthierarchylevel4text, producthierarchylevel5, producthierarchylevel5text, producthierarchylevel6, producthierarchylevel6text, globalcluster, lifecyclephase, packagesize, brandcode, branddescription, hybridvalue, approvedformanualexclusion, businessownerapproved,
            filename, dkin_countrycode, created_date, "updatedDate", productname, "quantityPerShippingUnit", "businessGroup", "lineOfBusiness", packagingcode)
            select distinct     materialid,     materialnumber, "source",material_desc, baseunitofmeasure,      materialgroup,  materialgroupdescription,
            materialgroupdescriptionlong,       materialtype,   materialproducthierarchy,       cast(materialrecordcreatedate as date),
            cast(materialrecordchangeddate as date),    traitcode,      crossplantmaterialstatus,       industrystandarddescription,
            productioninspectionmemo,   deleted,        productgroup,   producthierarchylevel1, producthierarchylevel1text,
            producthierarchylevel2,     producthierarchylevel2text,     producthierarchylevel3, producthierarchylevel3text,
            producthierarchylevel4,     producthierarchylevel4text,     producthierarchylevel5, producthierarchylevel5text,
            producthierarchylevel6,     producthierarchylevel6text,     globalcluster,  lifecyclephase, unitofmeasureforprimarypackaging,
            brandcode,  brandname,    hybridvalue,      approvedformanualexclusion,     businessownerapproved,    filename,
            dkin_countrycode,    current_date   created_date,   current_date "updatedDate",    productname, quantitypershippingunit,
            businessgroup,    lineofbusiness,    packagingcode
            from
            material_poc.in_stg_material_master
            ON CONFLICT on constraint md_dkins_material_tgt_v2_pk
            /* or you may use [DO NOTHING;] */
            DO UPDATE
            SET materialid=EXCLUDED.materialid, "Materialdescription"=EXCLUDED."Materialdescription",
            baseunitofmeasure=EXCLUDED.baseunitofmeasure, materialgroup=EXCLUDED.materialgroup,
            materialgroupdescription=EXCLUDED.materialgroupdescription,
            materialgroupdescriptionlong=EXCLUDED.materialgroupdescriptionlong, materialtype=EXCLUDED.materialtype,
            materialproducthierarchy=EXCLUDED.materialproducthierarchy, materialrecordcreatedate=EXCLUDED.materialrecordcreatedate,
            materialrecordchangeddate=EXCLUDED.materialrecordchangeddate, traitcode=EXCLUDED.traitcode,
            crossplantmaterialstatus=EXCLUDED.crossplantmaterialstatus, industrystandarddescription=EXCLUDED.industrystandarddescription, productioninspectionmemo=EXCLUDED.productioninspectionmemo, deleted=EXCLUDED.deleted, productgroup=EXCLUDED.productgroup, producthierarchylevel1=EXCLUDED.producthierarchylevel1, producthierarchylevel1text=EXCLUDED.producthierarchylevel1text, producthierarchylevel2=EXCLUDED.producthierarchylevel2, producthierarchylevel2text=EXCLUDED.producthierarchylevel2text, producthierarchylevel3=EXCLUDED.producthierarchylevel3, producthierarchylevel3text=EXCLUDED.producthierarchylevel3text, producthierarchylevel4=EXCLUDED.producthierarchylevel4, producthierarchylevel4text=EXCLUDED.producthierarchylevel4text, producthierarchylevel5=EXCLUDED.producthierarchylevel5, producthierarchylevel5text=EXCLUDED.producthierarchylevel5text, producthierarchylevel6=EXCLUDED.producthierarchylevel6, producthierarchylevel6text=EXCLUDED.producthierarchylevel6text, globalcluster=EXCLUDED.globalcluster, lifecyclephase=EXCLUDED.lifecyclephase, packagesize=EXCLUDED.packagesize, brandcode=EXCLUDED.brandcode, branddescription=EXCLUDED.branddescription, hybridvalue=EXCLUDED.hybridvalue, approvedformanualexclusion=EXCLUDED.approvedformanualexclusion, businessownerapproved=EXCLUDED.businessownerapproved, filename=EXCLUDED.filename, "updatedDate"=EXCLUDED."updatedDate", productname=EXCLUDED.productname, "quantityPerShippingUnit"=EXCLUDED."quantityPerShippingUnit", "businessGroup"=EXCLUDED."businessGroup",
            "lineOfBusiness"=EXCLUDED."lineOfBusiness", packagingcode=EXCLUDED.packagingcode;"""

            cursor.execute(sql_cmd2)

            sql_cmd3 = """INSERT INTO material_poc."MD_DKINS_MATERIAL_SALES_ORG_TGT"
            (materialnumber, sales_org, sales_distributionchannel, sales_unitofmeasure, salestexts_text, division,
            salestexts_language, filename, dkin_countrycode, created_date, sales_materialgroupfive, sales_materialgroupfivedescription,
            last_updated_date)
            select distinct
            materialnumber,
            case when salesorg is null then 'NULL' else salesorg end sales_org,
            case when distributionchannel is null then 'NULL' else distributionchannel end distributionchannel,
            case when sales_uom is null then 'NULL' else sales_uom end sales_uom,
            case when sales_text is null then 'NULL' else sales_text end sales_text ,
            case when division  is null then 'NULL' else division end sales_text ,
            case when sales_language is null then 'NULL' else sales_language end sales_language,
            filename,dkin_countrycode,
            current_date as created_date,
            case when materialgroupfive is null then 'NULL' else materialgroupfive end materialgroupfive,
            'NULL',current_date as last_updated_date
            from material_poc.in_stg_material_master
            ON CONFLICT on CONSTRAINT sales_v2
            /* or you may use [DO NOTHING;] */
            DO UPDATE
            SET filename=EXCLUDED.filename,last_updated_date=EXCLUDED.last_updated_date;"""

            cursor.execute(sql_cmd3)

            sql_cmd4 = """INSERT INTO material_poc."MD_DKINS_MATERIAL_UOM_TGT"
            (materialnumber, alternateunitofmeasure, conversionfactor, category, displayunitofmeasure,
            internationalarticlenumber, articlenumbercategory, uom_volume, uom_volumeunit, uom_displayvolumeunit, grossweight,
            uom_weightunit, displayweightunit, dimensions_weightunit, dimensions_weightnet, dimensions_weightgross,
            dimensions_volume, dimensions_volumeunit, dimensions_sizedimensions, filename, dkin_countrycode, created_date,
            last_updated_date)
            select distinct materialnumber, alternateunitofmeasure, conversionfactor, category,
            displayunitofmeasure, eaninfo_internationalarticlenumber, eaninfo_articlenumbercategory,
            uom_volume, uom_volumeunit, uom_displayvolumeunit, grossweight, weightunit, displayweightunit,
            dim_weightunit, dim_weightnet, dim_weightgross, dim_volume, dim_volumeunit,
            dim_sizedimensions, filename, dkin_countrycode, current_date created_date,
            current_date last_updated_date from material_poc.in_stg_material_master
            ON CONFLICT (materialnumber, alternateunitofmeasure, dkin_countrycode)
            /* or you may use [DO NOTHING;] */
            DO UPDATE
            SET conversionfactor=EXCLUDED.conversionfactor, category=EXCLUDED.category,
            displayunitofmeasure=EXCLUDED.displayunitofmeasure, internationalarticlenumber=EXCLUDED.internationalarticlenumber, articlenumbercategory=EXCLUDED.articlenumbercategory, uom_volume=EXCLUDED.uom_volume, uom_volumeunit=EXCLUDED.uom_volumeunit, uom_displayvolumeunit=EXCLUDED.uom_displayvolumeunit, grossweight=EXCLUDED.grossweight, uom_weightunit=EXCLUDED.uom_weightunit, displayweightunit=EXCLUDED.displayweightunit, dimensions_weightunit=EXCLUDED.dimensions_weightunit, dimensions_weightnet=EXCLUDED.dimensions_weightnet, dimensions_weightgross=EXCLUDED.dimensions_weightgross, dimensions_volume=EXCLUDED.dimensions_volume, dimensions_volumeunit=EXCLUDED.dimensions_volumeunit, dimensions_sizedimensions=EXCLUDED.dimensions_sizedimensions,
            filename=EXCLUDED.filename, last_updated_date=current_date;"""

            cursor.execute(sql_cmd4)

            conn.commit()
            cursor.close()
            conn.close()

            et = datetime.now()
            s4 = 'File' + jfile + 'Loaded Successfully...!'
            print(s4)
            print(st)
            print(et)
