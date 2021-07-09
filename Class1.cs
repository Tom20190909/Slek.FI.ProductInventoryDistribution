using Kingdee.BOS;
using Kingdee.BOS.App.Data;
using Kingdee.BOS.Contracts;
using Kingdee.BOS.Contracts.Report;
using Kingdee.BOS.Core.Metadata;
using Kingdee.BOS.Core.Metadata.FieldElement;
using Kingdee.BOS.Core.Metadata.GroupElement;
using Kingdee.BOS.Core.Report;
using Kingdee.BOS.Core.Report.PivotReport;
using Kingdee.BOS.Core.SqlBuilder;
using Kingdee.BOS.Orm.DataEntity;
using Kingdee.BOS.Util;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Text;

namespace Slek.FI.ProductInventoryDistribution
{
	[Description("产成品库存实时分布透视表")]
	[HotUpdate]
	public class ProductInventoryDistribution : SysReportBaseService
	{
		public override void Initialize()
		{
			base.ReportProperty.IsGroupSummary = false;

			base.ReportProperty.ReportName = new LocaleValue("产成品库存实时分布透视表");
			base.Initialize();

		}
		private string GetMultiOrgnNameValues(string orgIdStrings)
		{
			List<string> list = new List<string>();
			string result = string.Empty;
			if (orgIdStrings.Trim().Length > 0)
			{
				IQueryService service = Kingdee.BOS.Contracts.ServiceFactory.GetService<IQueryService>(base.Context);
				QueryBuilderParemeter para = new QueryBuilderParemeter
				{
					FormId = "ORG_Organizations",
					SelectItems = SelectorItemInfo.CreateItems("FNAME"),
					FilterClauseWihtKey = string.Format(" FORGID IN ({0}) AND FLOCALEID={1}", orgIdStrings, base.Context.UserLocale.LCID)
				};
				DynamicObjectCollection dynamicObjectCollection = service.GetDynamicObjectCollection(base.Context, para, null);
				foreach (DynamicObject current in dynamicObjectCollection)
				{
					list.Add(current["FNAME"].ToString());
				}
				if (list.Count > 0)
				{
					result = string.Join(",", list.ToArray());
				}
			}
			return result;
		}
		private string[] GetFilterWherePeriod(IRptParams filter)
		{
			DynamicObject customFilter = filter.FilterParameter.CustomFilter;
			
			string startValue = (customFilter["F_S_Date"] == null) ? "2000-01-01" : Convert.ToDateTime(customFilter["F_S_Date"]).ToString("yyyy-MM-dd");
			string endValue = (customFilter["F_E_Date"] == null) ? "2100-01-01" : Convert.ToDateTime(customFilter["F_E_Date"]).ToString("yyyy-MM-dd");
			
			//DynamicObjectCollection dyCollection = customFilter["F_FC_BaseMat"] as DynamicObjectCollection;
			//foreach (DynamicObject dobj in dyCollection)
			//{
			//	//long matId = Convert.ToInt64(dobj[0]);

			//	DynamicObject d = dobj["F_FC_BaseMat"] as DynamicObject;
			//	string Number = d["Number"].ToString();
				
			//}
			return new string[] { startValue, endValue };
		}
		public override void BuilderReportSqlAndTempTable(IRptParams filter, string tableName)
		{
			DynamicObject customFilter = filter.FilterParameter.CustomFilter;
			List<string> lstNumber = new List<string>();
			DynamicObjectCollection dyCollection = customFilter["F_FC_BaseMat"] as DynamicObjectCollection;
			foreach (DynamicObject dobj in dyCollection)
			{
				//long matId = Convert.ToInt64(dobj[0]);

				DynamicObject d = dobj["F_FC_BaseMat"] as DynamicObject;
				lstNumber.Add(d["Number"].ToString());

			}
			string whereNumber = string.Join("','",lstNumber);
			string[] Filter = GetFilterWherePeriod(filter);
			base.SettingInfo = new PivotReportSettingInfo();

			//string sql = string.Format(@"/*dialect*/ select ROW_NUMBER() OVER(ORDER BY a.物料编号,仓库) FIDENTITYID , a.*,b.可用库存总量,b.总库存量 into {0} from V_invstock_all a left join V_invstock_total b on a.物料编号=b.物料编号", tableName);
			string sql = string.Format(@"/*dialect*/select FIDENTITYID ,t0.物料编号,t0.物料名称,仓库,合格库存,待检库存,订单占用,可用库存总量,总库存量,cast(isnull(t2.销售数量,0) as     int) 销售总量,
					case  工作天数 when 0 then 0 else   cast(isnull(t2.销售数量,0) as int)/工作天数 end 日均销量,
					case  (cast(isnull(t2.销售数量,0) as int)/工作天数) when 0 then 0  else   cast(可用库存总量/(cast(isnull(t2.销售数量,0) as int)/工作天数) as numeric(18,1)) end 库存可用天数,最初销售日期 into {0}
					 from 
					(
					select ROW_NUMBER() OVER(ORDER BY a.物料编号,仓库) FIDENTITYID ,a.*,b.可用库存总量,b.总库存量,'A'  ts
					--,c.daycount  
					from V_invstock_all1 a left join V_invstock_total1 b on a.物料编号=b.物料编号
					) t0
					left join (
					
					select top  100000000  FNUMBER,dbo.GetWorkDays(min(FDATE),GETDATE()) 工作天数,convert(varchar(10),min(FDATE),120)  最初销售日期  from 
					(
					select  b.FBILLNO,c.FNUMBER,b.FDOCUMENTSTATUS,b.FDATE
					from          T_SAL_OUTSTOCKENTRY a
					left join     T_SAL_OUTSTOCK      b on a.FID=b.FID
					left join     T_BD_MATERIAL       c on a.FMATERIALID=c.FMATERIALID
					left join     T_BD_CUSTOMER_L     e on b.FCUSTOMERID=e.FCUSTID and e.FLOCALEID=2052
					where b.FDOCUMENTSTATUS='C' and e.FNAME not in ('舒美个人护理用品(深圳)有限公司','舒芙雅生物科技有限公司','舒蕾个人护理用品有限公司','舒颜日化(武汉)有限公司')
					) e group by FNUMBER order by FNUMBER
					) t1 
					on t0.物料编号=t1.FNUMBER
					left join 
					(
					select 
					j.fnumber 物料编号,
					e.fname 物料名称,
				    sum(a.FRealQty)  销售数量
					from T_SAL_OUTSTOCKENTRY  a
				 inner join T_SAL_OUTSTOCK b on a.fid=b.fid 
				 --sum(a.FPriceQty)  销售数量
					--from t_AR_receivableEntry a 
					--left join t_AR_receivable b on a.fid=b.fid 
					left join V_customer c on b.FCUSTOMERID=c.fcustid  
					left join  T_BD_CUSTOMER d on  c.FCUSTID=d.FCUSTID
					LEFT JOIN T_BD_MATERIAL_L e on  a.FMATERIALID=e.FMATERIALID
					LEFT JOIN T_BD_MATERIAL j on  a.FMATERIALID=j.FMATERIALID
					where 
					
					   b.FDOCUMENTSTATUS='C' and c.fname   not in ('舒美个人护理用品(深圳)有限公司','舒芙雅生物科技有限公司','舒蕾个人护理用品有限公司','舒颜日化(武汉)有限公司')
					   group by j.fnumber,e.fname 
					) t2 on t0.物料编号=t2.物料编号 where  t0.物料编号 in ('{1}')", tableName, whereNumber);
			DBUtils.ExecuteDynamicObject(this.Context, sql);

			//构造透视表列
			SettingField field0 = PivotReportSettingInfo.CreateColumnSettingField(new TextField()
			{
				Key = "物料编号",
				FieldName = "物料编号",
				Name = new LocaleValue("物料编号")

			}, 0);
			field0.IsShowTotal = false;
			base.SettingInfo.RowTitleFields.Add(field0);
			base.SettingInfo.SelectedFields.Add(field0);


			SettingField field1 = PivotReportSettingInfo.CreateColumnSettingField(new TextField()
			{
				Key = "物料名称",
				FieldName = "物料名称",
				Name = new LocaleValue("物料名称")
			}, 1);
			field1.IsShowTotal = false;
			base.SettingInfo.RowTitleFields.Add(field1);
			base.SettingInfo.SelectedFields.Add(field1);


			SettingField field20 = PivotReportSettingInfo.CreateColumnSettingField(new TextField()
			{
				Key = "最初销售日期",
				FieldName = "最初销售日期",
				Name = new LocaleValue("最初销售日期")
			}, 2);
			field20.IsShowTotal = false;
			base.SettingInfo.RowTitleFields.Add(field20);
			base.SettingInfo.SelectedFields.Add(field20);

			SettingField field21 = PivotReportSettingInfo.CreateColumnSettingField(new DecimalField()
			{
				Key = "销售总量",
				FieldName = "销售总量",
				Name = new LocaleValue("销售总量")
			}, 2);
			field21.IsShowTotal = false;
			base.SettingInfo.RowTitleFields.Add(field21);
			base.SettingInfo.SelectedFields.Add(field21);

			SettingField field22 = PivotReportSettingInfo.CreateColumnSettingField(new DecimalField()
			{
				Key = "日均销量",
				FieldName = "日均销量",
				Name = new LocaleValue("日均销量")
			}, 3);
			field22.IsShowTotal = false;
			base.SettingInfo.RowTitleFields.Add(field22);
			base.SettingInfo.SelectedFields.Add(field22);

			SettingField field23 = PivotReportSettingInfo.CreateColumnSettingField(new DecimalField()
			{
				Key = "库存可用天数",
				FieldName = "库存可用天数",
				Name = new LocaleValue("库存可用天数")
			}, 4);
			field23.IsShowTotal = false;
			base.SettingInfo.RowTitleFields.Add(field23);
			base.SettingInfo.SelectedFields.Add(field23);


			SettingField field3 = PivotReportSettingInfo.CreateColumnSettingField(new IntegerField()
			{
				Key = "可用库存总量",
				FieldName = "可用库存总量",
				Name = new LocaleValue("可用库存总量(在库+待检-订单占用)")
			}, 20);
			field3.IsShowTotal = false;
			base.SettingInfo.RowTitleFields.Add(field3);
			base.SettingInfo.SelectedFields.Add(field3);

			SettingField field2 = PivotReportSettingInfo.CreateColumnSettingField(new IntegerField()
			{
				Key = "总库存量",
				FieldName = "总库存量",
				Name = new LocaleValue("总库存量(在库+待检)")
			}, 30);
			field2.IsShowTotal = false;
			base.SettingInfo.RowTitleFields.Add(field2);
			base.SettingInfo.SelectedFields.Add(field2);

			

			SettingField field4 = PivotReportSettingInfo.CreateDataSettingField(new DecimalField()
			{
				Key = "合格库存",
				FieldName = "合格库存",
				Name = new LocaleValue("合格库存")
			}, 0, GroupSumType.Sum,"N0");
			//field4.SumType = 1;
			// field5.IsShowTotal = false;
			base.SettingInfo.AggregateFields.Add(field4);
			base.SettingInfo.SelectedFields.Add(field4);

			SettingField field5 = PivotReportSettingInfo.CreateDataSettingField(new DecimalField()
			{
				Key = "待检库存",
				FieldName = "待检库存",
				Name = new LocaleValue("待检库存")
			},1, GroupSumType.Sum, "N0");
			field5.SumType = 1;
			// field5.IsShowTotal = false;
			base.SettingInfo.AggregateFields.Add(field5);
			base.SettingInfo.SelectedFields.Add(field5);

			SettingField field6 = PivotReportSettingInfo.CreateDataSettingField(new IntegerField()
			{
				Key = "订单占用",
				FieldName = "订单占用",
				Name = new LocaleValue("订单占用")
			}, 2, GroupSumType.Sum, "N0");
			field6.SumType = 1;
			// field5.IsShowTotal = false;
			base.SettingInfo.AggregateFields.Add(field6);
			base.SettingInfo.SelectedFields.Add(field6);
			
			SettingField field7 = PivotReportSettingInfo.CreateColumnSettingField(new TextField()
			{
				Key = "仓库",
				FieldName = "仓库",
				Name = new LocaleValue("仓库")
			}, 0);

			base.SettingInfo.ColTitleFields.Add(field7);
			base.SettingInfo.SelectedFields.Add(field7);
		//
		
		}
	}
}
