package jiq.hbase;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.util.Bytes;

import jiq.security.LoginUtil;

public class HBase {
	private final static Log LOG = LogFactory.getLog(HBase.class.getName());

	private TableName tableName = null;
	private Configuration conf = null;
	private Connection conn = null;

	/**
	 * 创建Connection<br>
	 * HBase通过ConnectionFactory.createConnection(configuration)方法创建Connection对象。传递的参数为上一步创建的Configuration。<br>
	 * Connection封装了底层与各实际服务器的连接以及与ZooKeeper的连接。Connection通过ConnectionFactory类实例化。
	 * 创建Connection是重量级操作，Connection是线程安全的，因此，多个客户端线程可以共享一个Connection。<br>
	 * 典型的用法，一个客户端程序共享一个单独的Connection，每一个线程获取自己的Admin或Table实例，然后调用Admin对象或Table对象提供的操作接口。
	 * 不建议缓存或者池化Table、Admin。Connection的生命周期由调用者维护，调用者通过调用close()，释放资源。
	 */
	public HBase(Configuration conf) throws IOException {
		this.conf = conf;
		this.tableName = TableName.valueOf("hbase_sample_table");
		this.conn = ConnectionFactory.createConnection(conf);
	}

	public static void main(String[] args) {
		String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
		// 创建Configuration。 HBase通过login方法来获取配置项。包括用户登录信息、安全认证信息等配置项。
		Configuration configuration = HBaseConfiguration.create();
		configuration.addResource(new Path(userdir + "core-site.xml"));
		configuration.addResource(new Path(userdir + "hdfs-site.xml"));
		configuration.addResource(new Path(userdir + "hbase-site.xml"));
		try {
			LoginUtil.setJaasConf("Client", "jiq", userdir + "user.keytab");
			LoginUtil.setZookeeperServerPrincipal("zookeeper.server.principal", "zookeeper.server.principal");
			LoginUtil.login("jiq", userdir + "user.keytab", userdir + "krb5.conf", configuration);
		} catch (IOException e) {
			LOG.error("Failed to login because ", e);
			return;
		}

		try {
			new HBase(configuration).run();
		} catch (Exception e) {
			LOG.error("Failed to run HBase because ", e);
		}
		LOG.info("-----------finish HBase -------------------");
	}

	public void run() throws Exception {
		try {
			createTable();
			multiSplit();
			put();
			createIndex();
			scanDataByIndex();
			modifyTable();
			get();
			scanData();
			singleColumnValueFilter();
			filterList();
			delete();
			dropIndex();
			dropTable();
			createMOBTable();
			mobDataInsertion();
			mobDataRead();
			dropTable();
		} catch (Exception e) {
			throw e;
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e1) {
					LOG.error("Failed to close the connection ", e1);
				}
			}
		}
	}

	/**
	 * 创建表<br>
	 * HBase通过org.apache.hadoop.hbase.client.Admin对象的createTable方法来创建表，并指定表名、列族名。创建表有两种方式（强烈建议采用预分Region建表方式）：
	 * <ol>
	 * <li>快速建表，即创建表后整张表只有一个Region，随着数据量的增加会自动分裂成多个Region。</li>
	 * <li>预分Region建表，即创建表时预先分配多个Region，此种方法建表可以提高写入大量数据初期的数据写入速度。</li>
	 * </ol>
	 */
	public void createTable() {
		LOG.info("Entering createTable.");

		// 创建表描述符
		HTableDescriptor htd = new HTableDescriptor(tableName);

		// 创建列族描述符
		HColumnDescriptor hcd = new HColumnDescriptor("info");

		// 设置编码算法，HBase提供了DIFF，FAST_DIFF，PREFIX和PREFIX_TREE四种编码算法
		hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);

		// 设置文件压缩方式，HBase默认提供了GZ和SNAPPY两种压缩算法
		// 其中GZ的压缩率高，但压缩和解压性能低，适用于冷数据
		// SNAPPY压缩率低，但压缩解压性能高，适用于热数据
		// 建议默认开启SNAPPY压缩
		hcd.setCompressionType(Compression.Algorithm.SNAPPY);

		// 添加列族描述符到表描述符中
		htd.addFamily(hcd);

		Admin admin = null;
		try {
			// 获取Admin对象，Admin提供了建表、创建列族、检查表是否存在、修改表结构和列族结构以及删除表等功能。
			admin = conn.getAdmin();
			if (!admin.tableExists(tableName)) {
				LOG.info("Creating table...");
				// 调用Admin的建表方法
				admin.createTable(htd);
				// 可以通过指定起始和结束RowKey，或者通过RowKey数组预分Region两种方式建表，代码片段如下：
				// 创建一个预划分region的表
				// byte[][] splits = new byte[4][];
				// splits[0] = Bytes.toBytes("A");
				// splits[1] = Bytes.toBytes("H");
				// splits[2] = Bytes.toBytes("O");
				// splits[3] = Bytes.toBytes("U");
				// admin.createTable(htd, splits);
				LOG.info(admin.getClusterStatus());
				LOG.info(admin.listNamespaceDescriptors());
				LOG.info("Table created successfully.");
			} else {
				LOG.warn("table already exists");
			}
		} catch (IOException e) {
			LOG.error("Create table failed.", e);
		} finally {
			if (admin != null) {
				try {
					// 关闭Admin对象
					admin.close();
				} catch (IOException e) {
					LOG.error("Failed to close admin ", e);
				}
			}
		}
		LOG.info("Exiting createTable.");
	}

	public void multiSplit() {
		LOG.info("Entering testMultiSplit.");

		Table table = null;
		Admin admin = null;
		try {
			admin = conn.getAdmin();

			// initilize a HTable object
			table = conn.getTable(tableName);
			Set<HRegionInfo> regionSet = new HashSet<HRegionInfo>();
			List<HRegionLocation> regionList = conn.getRegionLocator(tableName).getAllRegionLocations();
			for (HRegionLocation hrl : regionList) {
				regionSet.add(hrl.getRegionInfo());
			}
			byte[][] sk = new byte[4][];
			sk[0] = "A".getBytes();
			sk[1] = "D".getBytes();
			sk[2] = "F".getBytes();
			sk[3] = "H".getBytes();
			for (HRegionInfo regionInfo : regionSet) {
				((HBaseAdmin) admin).multiSplit(regionInfo.getRegionName(), sk);
			}
			LOG.info("MultiSplit successfully.");
		} catch (Exception e) {
			LOG.error("MultiSplit failed ", e);
		} finally {
			if (table != null) {
				try {
					// Close table object
					table.close();
				} catch (IOException e) {
					LOG.error("Close table failed ", e);
				}
			}
			if (admin != null) {
				try {
					// Close the Admin object.
					admin.close();
				} catch (IOException e) {
					LOG.error("Close admin failed ", e);
				}
			}
		}
		LOG.info("Exiting testMultiSplit.");
	}

	/**
	 * Insert data
	 */
	public void put() {
		LOG.info("Entering testPut.");

		// Specify the column family name.
		byte[] familyName = Bytes.toBytes("info");
		// Specify the column name.
		byte[][] qualifiers = { Bytes.toBytes("name"), Bytes.toBytes("gender"), Bytes.toBytes("age"),
				Bytes.toBytes("address") };

		Table table = null;
		try {
			// Instantiate an HTable object.
			table = conn.getTable(tableName);
			List<Put> puts = new ArrayList<Put>();
			// Instantiate a Put object.
			Put put = new Put(Bytes.toBytes("012005000201"));
			put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Zhang San"));
			put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
			put.addColumn(familyName, qualifiers[2], Bytes.toBytes("19"));
			put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Shenzhen, Guangdong"));
			puts.add(put);

			put = new Put(Bytes.toBytes("012005000202"));
			put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Li Wanting"));
			put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Female"));
			put.addColumn(familyName, qualifiers[2], Bytes.toBytes("23"));
			put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Shijiazhuang, Hebei"));
			puts.add(put);

			put = new Put(Bytes.toBytes("012005000203"));
			put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Wang Ming"));
			put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
			put.addColumn(familyName, qualifiers[2], Bytes.toBytes("26"));
			put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Ningbo, Zhejiang"));
			puts.add(put);

			put = new Put(Bytes.toBytes("012005000204"));
			put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Li Gang"));
			put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
			put.addColumn(familyName, qualifiers[2], Bytes.toBytes("18"));
			put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Xiangyang, Hubei"));
			puts.add(put);

			put = new Put(Bytes.toBytes("012005000205"));
			put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Zhao Enru"));
			put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Female"));
			put.addColumn(familyName, qualifiers[2], Bytes.toBytes("21"));
			put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Shangrao, Jiangxi"));
			puts.add(put);

			put = new Put(Bytes.toBytes("012005000206"));
			put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Chen Long"));
			put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
			put.addColumn(familyName, qualifiers[2], Bytes.toBytes("32"));
			put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Zhuzhou, Hunan"));
			puts.add(put);

			put = new Put(Bytes.toBytes("012005000207"));
			put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Zhou Wei"));
			put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Female"));
			put.addColumn(familyName, qualifiers[2], Bytes.toBytes("29"));
			put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Nanyang, Henan"));
			puts.add(put);

			put = new Put(Bytes.toBytes("012005000208"));
			put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Yang Yiwen"));
			put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Female"));
			put.addColumn(familyName, qualifiers[2], Bytes.toBytes("30"));
			put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Kaixian, Chongqing"));
			puts.add(put);

			put = new Put(Bytes.toBytes("012005000209"));
			put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Xu Bing"));
			put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
			put.addColumn(familyName, qualifiers[2], Bytes.toBytes("26"));
			put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Weinan, Shaanxi"));
			puts.add(put);

			put = new Put(Bytes.toBytes("012005000210"));
			put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Xiao Kai"));
			put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
			put.addColumn(familyName, qualifiers[2], Bytes.toBytes("25"));
			put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Dalian, Liaoning"));
			puts.add(put);

			// Submit a put request.
			table.put(puts);

			LOG.info("Put successfully.");
		} catch (IOException e) {
			LOG.error("Put failed ", e);
		} finally {
			if (table != null) {
				try {
					// Close the HTable object.
					table.close();
				} catch (IOException e) {
					LOG.error("Close table failed ", e);
				}
			}
		}
		LOG.info("Exiting testPut.");
	}

	public void createIndex() {
		LOG.info("Entering createIndex.");

		String indexName = "index_name";

		// Create index instance
		IndexSpecification iSpec = new IndexSpecification(indexName);

		iSpec.addIndexColumn(new HColumnDescriptor("info"), "name", ValueType.String);

		IndexAdmin iAdmin = null;
		Admin admin = null;
		try {
			// Instantiate IndexAdmin Object
			iAdmin = new IndexAdmin(conf);

			// Create Secondary Index
			iAdmin.addIndex(tableName, iSpec);

			admin = conn.getAdmin();

			// Specify the encryption type of indexed column
			HTableDescriptor htd = admin.getTableDescriptor(tableName);
			admin.disableTable(tableName);
			htd = admin.getTableDescriptor(tableName);
			// Instantiate index column description.
			HColumnDescriptor indexColDesc = new HColumnDescriptor(IndexMasterObserver.DEFAULT_INDEX_COL_DESC);

			// Set the description of index as the HTable description.
			htd.setValue(Constants.INDEX_COL_DESC_BYTES, indexColDesc.toByteArray());
			admin.modifyTable(tableName, htd);
			admin.enableTable(tableName);

			LOG.info("Create index successfully.");

		} catch (IOException e) {
			LOG.error("Create index failed.", e);
		} finally {
			if (admin != null) {
				try {
					admin.close();
				} catch (IOException e) {
					LOG.error("Close admin failed ", e);
				}
			}
			if (iAdmin != null) {
				try {
					// Close IndexAdmin Object
					iAdmin.close();
				} catch (IOException e) {
					LOG.error("Close admin failed ", e);
				}
			}
		}
		LOG.info("Exiting createIndex.");
	}

	/**
	 * Scan data by secondary index.
	 */
	public void scanDataByIndex() {
		LOG.info("Entering testScanDataByIndex.");

		Table table = null;
		ResultScanner scanner = null;
		try {
			table = conn.getTable(tableName);

			// Create a filter for indexed column.
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"), CompareOp.EQUAL,
					"Li Gang".getBytes());
			Scan scan = new Scan();
			scan.setFilter(filter);
			scanner = table.getScanner(scan);
			LOG.info("Scan indexed data.");

			for (Result result : scanner) {
				for (Cell cell : result.rawCells()) {
					LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell))
							+ "," + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
							+ Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}
			LOG.info("Scan data by index successfully.");
		} catch (IOException e) {
			LOG.error("Scan data by index failed ", e);
		} finally {
			if (scanner != null) {
				// Close the scanner object.
				scanner.close();
			}
			try {
				if (table != null) {
					table.close();
				}
			} catch (IOException e) {
				LOG.error("Close table failed ", e);
			}
		}

		LOG.info("Exiting testScanDataByIndex.");
	}

	/**
	 * Modify a Table
	 */
	public void modifyTable() {
		LOG.info("Entering testModifyTable.");

		// Specify the column family name.
		byte[] familyName = Bytes.toBytes("education");

		Admin admin = null;
		try {
			// Instantiate an Admin object.
			admin = conn.getAdmin();

			// Obtain the table descriptor.
			HTableDescriptor htd = admin.getTableDescriptor(tableName);

			// Check whether the column family is specified before modification.
			if (!htd.hasFamily(familyName)) {
				// Create the column descriptor.
				HColumnDescriptor hcd = new HColumnDescriptor(familyName);
				htd.addFamily(hcd);

				// Disable the table to get the table offline before modifying
				// the table.
				admin.disableTable(tableName);
				// Submit a modifyTable request.
				admin.modifyTable(tableName, htd);
				// Enable the table to get the table online after modifying the
				// table.
				admin.enableTable(tableName);
			}
			LOG.info("Modify table successfully.");
		} catch (IOException e) {
			LOG.error("Modify table failed ", e);
		} finally {
			if (admin != null) {
				try {
					// Close the Admin object.
					admin.close();
				} catch (IOException e) {
					LOG.error("Close admin failed ", e);
				}
			}
		}
		LOG.info("Exiting testModifyTable.");
	}

	/**
	 * Get Data
	 */
	public void get() {
		LOG.info("Entering testGet.");

		// Specify the column family name.
		byte[] familyName = Bytes.toBytes("info");
		// Specify the column name.
		byte[][] qualifier = { Bytes.toBytes("name"), Bytes.toBytes("address") };
		// Specify RowKey.
		byte[] rowKey = Bytes.toBytes("012005000201");

		Table table = null;
		try {
			// Create the Configuration instance.
			table = conn.getTable(tableName);

			// Instantiate a Get object.
			Get get = new Get(rowKey);

			// Set the column family name and column name.
			get.addColumn(familyName, qualifier[0]);
			get.addColumn(familyName, qualifier[1]);

			// Submit a get request.
			Result result = table.get(get);

			// Print query results.
			for (Cell cell : result.rawCells()) {
				LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell))
						+ "," + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
						+ Bytes.toString(CellUtil.cloneValue(cell)));
			}
			LOG.info("Get data successfully.");
		} catch (IOException e) {
			LOG.error("Get data failed ", e);
		} finally {
			if (table != null) {
				try {
					// Close the HTable object.
					table.close();
				} catch (IOException e) {
					LOG.error("Close table failed ", e);
				}
			}
		}
		LOG.info("Exiting testGet.");
	}

	public void scanData() {
		LOG.info("Entering testScanData.");

		Table table = null;
		// Instantiate a ResultScanner object.
		ResultScanner rScanner = null;
		try {
			// Create the Configuration instance.
			table = conn.getTable(tableName);

			// Instantiate a Get object.
			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

			// Set the cache size.
			scan.setCaching(1000);

			// Submit a scan request.
			rScanner = table.getScanner(scan);

			// Print query results.
			for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
				for (Cell cell : r.rawCells()) {
					LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell))
							+ "," + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
							+ Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}
			LOG.info("Scan data successfully.");
		} catch (IOException e) {
			LOG.error("Scan data failed ", e);
		} finally {
			if (rScanner != null) {
				// Close the scanner object.
				rScanner.close();
			}
			if (table != null) {
				try {
					// Close the HTable object.
					table.close();
				} catch (IOException e) {
					LOG.error("Close table failed ", e);
				}
			}
		}
		LOG.info("Exiting testScanData.");
	}

	public void singleColumnValueFilter() {
		LOG.info("Entering testSingleColumnValueFilter.");

		Table table = null;

		// Instantiate a ResultScanner object.
		ResultScanner rScanner = null;

		try {
			// Create the Configuration instance.
			table = conn.getTable(tableName);

			// Instantiate a Get object.
			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

			// Set the filter criteria.
			SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
					CompareOp.EQUAL, Bytes.toBytes("Xu Bing"));

			scan.setFilter(filter);

			// Submit a scan request.
			rScanner = table.getScanner(scan);

			// Print query results.
			for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
				for (Cell cell : r.rawCells()) {
					LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell))
							+ "," + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
							+ Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}
			LOG.info("Single column value filter successfully.");
		} catch (IOException e) {
			LOG.error("Single column value filter failed ", e);
		} finally {
			if (rScanner != null) {
				// Close the scanner object.
				rScanner.close();
			}
			if (table != null) {
				try {
					// Close the HTable object.
					table.close();
				} catch (IOException e) {
					LOG.error("Close table failed ", e);
				}
			}
		}
		LOG.info("Exiting testSingleColumnValueFilter.");
	}

	public void filterList() {
		LOG.info("Entering testFilterList.");

		Table table = null;

		// Instantiate a ResultScanner object.
		ResultScanner rScanner = null;

		try {
			// Create the Configuration instance.
			table = conn.getTable(tableName);

			// Instantiate a Get object.
			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

			// Instantiate a FilterList object in which filters have "and"
			// relationship with each other.
			FilterList list = new FilterList(Operator.MUST_PASS_ALL);
			// Obtain data with age of greater than or equal to 20.
			list.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("age"),
					CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(new Long(20))));
			// Obtain data with age of less than or equal to 29.
			list.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("age"),
					CompareOp.LESS_OR_EQUAL, Bytes.toBytes(new Long(29))));

			scan.setFilter(list);

			// Submit a scan request.
			rScanner = table.getScanner(scan);
			// Print query results.
			for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
				for (Cell cell : r.rawCells()) {
					LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell))
							+ "," + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
							+ Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}
			LOG.info("Filter list successfully.");
		} catch (IOException e) {
			LOG.error("Filter list failed ", e);
		} finally {
			if (rScanner != null) {
				// Close the scanner object.
				rScanner.close();
			}
			if (table != null) {
				try {
					// Close the HTable object.
					table.close();
				} catch (IOException e) {
					LOG.error("Close table failed ", e);
				}
			}
		}
		LOG.info("Exiting testFilterList.");
	}

	/**
	 * deleting data
	 */
	public void delete() {
		LOG.info("Entering testDelete.");

		byte[] rowKey = Bytes.toBytes("012005000201");

		Table table = null;
		try {
			// Instantiate an HTable object.
			table = conn.getTable(tableName);

			// Instantiate an Delete object.
			Delete delete = new Delete(rowKey);

			// Submit a delete request.
			table.delete(delete);

			LOG.info("Delete table successfully.");
		} catch (IOException e) {
			LOG.error("Delete table failed ", e);
		} finally {
			if (table != null) {
				try {
					// Close the HTable object.
					table.close();
				} catch (IOException e) {
					LOG.error("Close table failed ", e);
				}
			}
		}
		LOG.info("Exiting testDelete.");
	}

	public void dropIndex() {
		LOG.info("Entering dropIndex.");

		String indexName = "index_name";

		IndexAdmin iAdmin = null;
		try {
			// Instantiate IndexAdmin Object
			iAdmin = new IndexAdmin(conf);

			// Delete Secondary Index
			iAdmin.dropIndex(tableName, indexName);

			LOG.info("Drop index successfully.");
		} catch (IOException e) {
			LOG.error("Drop index failed ", e);
		} finally {
			if (iAdmin != null) {
				try {
					// Close Secondary Index
					iAdmin.close();
				} catch (IOException e) {
					LOG.error("Close admin failed ", e);
				}
			}
		}
		LOG.info("Exiting dropIndex.");
	}

	/**
	 * HBase通过org.apache.hadoop.hbase.client.Admin的deleteTable方法来删除表
	 */
	public void dropTable() {
		LOG.info("Entering dropTable.");

		Admin admin = null;
		try {
			admin = conn.getAdmin();
			if (admin.tableExists(tableName)) {
				// Disable the table before deleting it.
				admin.disableTable(tableName);

				// 删除表.只有表被disable时，才能被删除掉，所以deleteTable常与disableTable，enableTable，tableExists，isTableEnabled，isTableDisabled结合在一起使用。
				admin.deleteTable(tableName);
			}
			LOG.info("Drop table successfully.");
		} catch (IOException e) {
			LOG.error("Drop table failed ", e);
		} finally {
			if (admin != null) {
				try {
					// 关闭Admin对象.
					admin.close();
				} catch (IOException e) {
					LOG.error("Close admin failed ", e);
				}
			}
		}
		LOG.info("Exiting dropTable.");
	}

	public void grantACL() {
		LOG.info("Entering grantACL.");

		String user = "huawei";
		String permissions = "RW";

		String familyName = "info";
		String qualifierName = "name";

		Table mt = null;
		Admin hAdmin = null;
		try {
			// Create ACL Instance
			mt = conn.getTable(AccessControlLists.ACL_TABLE_NAME);

			Permission perm = new Permission(Bytes.toBytes(permissions));

			hAdmin = conn.getAdmin();
			HTableDescriptor ht = hAdmin.getTableDescriptor(tableName);

			// Judge whether the table exists
			if (hAdmin.tableExists(mt.getName())) {
				// Judge whether ColumnFamily exists
				if (ht.hasFamily(Bytes.toBytes(familyName))) {
					// grant permission
					AccessControlClient.grant(conn, tableName, user, Bytes.toBytes(familyName),
							(qualifierName == null ? null : Bytes.toBytes(qualifierName)), perm.getActions());
				} else {
					// grant permission
					AccessControlClient.grant(conn, tableName, user, null, null, perm.getActions());
				}
			}
			LOG.info("Grant ACL successfully.");
		} catch (Throwable e) {
			LOG.error("Grant ACL failed ", e);
		} finally {
			if (mt != null) {
				try {
					// Close
					mt.close();
				} catch (IOException e) {
					LOG.error("Close table failed ", e);
				}
			}

			if (hAdmin != null) {
				try {
					// Close Admin Object
					hAdmin.close();
				} catch (IOException e) {
					LOG.error("Close admin failed ", e);
				}
			}
		}
		LOG.info("Exiting grantACL.");
	}

	public void mobDataRead() {
		LOG.info("Entering testMOBDataRead.");
		ResultScanner scanner = null;
		Table table = null;
		Admin admin = null;
		try {

			// get table object representing table tableName
			table = conn.getTable(tableName);
			admin = conn.getAdmin();
			admin.flush(table.getName());
			Scan scan = new Scan();
			// get table scanner
			scanner = table.getScanner(scan);
			for (Result result : scanner) {
				byte[] value = result.getValue(Bytes.toBytes("mobcf"), Bytes.toBytes("cf1"));
				String string = Bytes.toString(value);
				LOG.info("value:" + string);
			}
			LOG.info("MOB data read successfully.");
		} catch (Exception e) {
			LOG.error("MOB data read failed ", e);
		} finally {
			if (scanner != null) {
				scanner.close();
			}
			if (table != null) {
				try {
					// Close table object
					table.close();
				} catch (IOException e) {
					LOG.error("Close table failed ", e);
				}
			}
			if (admin != null) {
				try {
					// Close the Admin object.
					admin.close();
				} catch (IOException e) {
					LOG.error("Close admin failed ", e);
				}
			}
		}
		LOG.info("Exiting testMOBDataRead.");
	}

	public void mobDataInsertion() {
		LOG.info("Entering testMOBDataInsertion.");

		Table table = null;
		try {
			// set row name to "row"
			Put p = new Put(Bytes.toBytes("row"));
			byte[] value = new byte[1000];
			// set the column value of column family mobcf with the value of
			// "cf1"
			p.addColumn(Bytes.toBytes("mobcf"), Bytes.toBytes("cf1"), value);
			// get the table object represent table tableName
			table = conn.getTable(tableName);
			// put data
			table.put(p);
			LOG.info("MOB data inserted successfully.");

		} catch (Exception e) {
			LOG.error("MOB data inserted failed ", e);
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (Exception e1) {
					LOG.error("Close table failed ", e1);
				}
			}
		}
		LOG.info("Exiting testMOBDataInsertion.");
	}

	public void createMOBTable() {
		LOG.info("Entering testCreateMOBTable.");

		Admin admin = null;
		try {
			// Create Admin instance
			admin = conn.getAdmin();
			HTableDescriptor tabDescriptor = new HTableDescriptor(tableName);
			HColumnDescriptor mob = new HColumnDescriptor("mobcf");
			// Open mob function
			mob.setMobEnabled(true);
			// Set mob threshold
			mob.setMobThreshold(10L);
			tabDescriptor.addFamily(mob);
			admin.createTable(tabDescriptor);
			LOG.info("MOB Table is created successfully.");

		} catch (Exception e) {
			LOG.error("MOB Table is created failed ", e);
		} finally {
			if (admin != null) {
				try {
					// Close the Admin object.
					admin.close();
				} catch (IOException e) {
					LOG.error("Close admin failed ", e);
				}
			}
		}
		LOG.info("Exiting testCreateMOBTable.");
	}

}
