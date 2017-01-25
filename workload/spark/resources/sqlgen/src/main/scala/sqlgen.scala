package src.main.scala

import com.google.common.io.{ByteStreams, Files}
import java.io.File
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.util.Random
import scala.math
import scala.collection.mutable.ListBuffer

object SQLGen {

    /*
     * The outlook of table:
     * OS_ORDER.txt:
     * order_id, bid, date
     * (1) order_id shall be some how random, unique, sequential
     * (2) bid type is long, shall be some offset added with order_id
     * (3) date composed of 30% "NULL" and 70% random date, sequential
     * 
     * OS_ORDER_ITEM.txt:
     * item_id, order_id, goods_id, goods_number, goods_price, goods_sell
     * (1) item_id is some how random, unique, sequential
     * (2) order_id comes from OS_ORDER table, sequential, with duplications
     * (3) goods_id and other fields are pure random data following some distribution
     */
    def main(args: Array[String]) {

        if (args.length != 3) {
            println("Usage: SQLGenV2Complex <count> <HDFS output Dir> <partition>")
            System.exit(1)
        }

	// Hard code all parameter at the moment
        //val order_count = 20000000
        //val item_count = 20000000
        val order_count = args(0).toInt
        val item_count = args(0).toInt
        val part_num = args(2).toInt
        val bid_start = 100300000023L
        val total_days = 900
        val orders_per_day = order_count / total_days
        val goods_id_start = 1010060
        val goods_id_range = order_count / 6
        val os_order_path = args(1) + "/OS_ORDER.txt"
        val os_order_item_path = args(1) + "/OS_ORDER_ITEM.txt"
        val exp_dist = Array()

        // magic numbers
        val choose_hundred_from:Long = 142L

        val sparkConf = new SparkConf().setAppName("SQLGenV2")
        val sc = new SparkContext(sparkConf)

        val order_table_dup_cnt = 1
        val item_table_dup_cnt = 1

        def pack_os_order_elem(oid:Int, bid:Long, date:String, dummy:String): (Int, Long, String, String) = {
            (oid, bid, date, dummy)
        }

        val order_table = sc.parallelize(0 to order_count, part_num).flatMap{idx =>
            val rand = new Random(42 + idx)
            var items = new ListBuffer[(Int, Long, String, String)]()
            val year = 2001 + (idx / orders_per_day) / 360
            val month = ((idx / orders_per_day) % 360) / 30 + 1
            val day = ((idx / orders_per_day) % 360) % 30 + 1
             for (i <- 0 to (order_table_dup_cnt - 1)) {
                val date = {
                    "%d-%d-%d %02d:%02d:%02d" format (day, month, year, rand.nextInt(24), rand.nextInt(60), rand.nextInt(60))
                }
                var dummy: String = ""
                for (j <- 0 to 50) {
                    dummy = dummy + rand.nextInt(9).toString
                }
                items += pack_os_order_elem(idx, (bid_start + idx), date, dummy)
            }
            items.toList
        }.map{case (a, b, c, d) => 
            a + "\t" + b + "\t" + c + "\t" + d}.saveAsTextFile(os_order_path)

        def pack_item_elem(iid:Int, oid:Int, flag:Int): (Int, Int, Int) = {
            (iid, oid, flag)
        }

        val batch_per_idx = 100
        val item_table = sc.parallelize(0 to (item_count * 1.02).toInt / batch_per_idx, part_num).flatMap{idx =>
            val rand = new Random(24 + idx)
            var items = new ListBuffer[(Int, Int, Int)]()
            var item_remain = batch_per_idx
            var item_idx = idx * batch_per_idx
            while (item_remain > 0) {
                val r = rand.nextInt(100)
                var item_order_ratio = 0
                if (r > 80) {
                    item_order_ratio = 10 + (r - 80)
                } else {
                    item_order_ratio = r % 10 + 1 
                }
                if (item_order_ratio > item_remain) {
                    item_order_ratio = item_remain
                    item_remain = 0
                } else {
                    item_remain = item_remain - item_order_ratio
                }
                val order_idx = idx * batch_per_idx + rand.nextInt(batch_per_idx)
                for (i <- 0 to (item_order_ratio - 1)) {
                    val keep_flag = {
                        if (rand.nextInt(1020) < 1000) {
                            1
                        } else {
                            0
                        }
                    } 
                    items += pack_item_elem(item_idx, order_idx, keep_flag)
                    item_idx = item_idx + 1
                }
            }
            items.toList
        }.filter{case (i, o, k) => k == 1}.map{case (i, o, k) =>
            val rand = new Random(i)
            val goods_id = goods_id_start + rand.nextInt(goods_id_range)
            val goods_num = 1 + rand.nextInt(1000)
            val goods_price = 1 + rand.nextInt(55000)
            val goods_price_show = "%.02f" format (goods_price / 100.0)
            val shop_price_show =  {
                if (rand.nextInt(10) < 9.0) {
                    goods_price_show
                } else {
                    val new_price = goods_price - rand.nextInt(goods_price) + 1
                    "%.02f" format (new_price / 100.0)
                }
            }
            var dummy: String = ""
            for (j <- 0 to 50) {
                dummy = dummy + rand.nextInt(9).toString
            }
            (i, o, goods_id, goods_num, goods_price_show, shop_price_show, dummy)
        }.map{case (i, o, goods_id, goods_num, goods_price_show, shop_price_show, dummy) => 
	    i + "\t" + o + "\t" + goods_id + "\t" + goods_num + ".00" + "\t" + goods_price_show + "\t" + shop_price_show + "\t" + dummy}.saveAsTextFile(os_order_item_path)

        System.exit(0)
    }
}
