#include "all.h"

#include "bench/tpcc_real_dist/sharding.h"

namespace rococo {


void TpccChopper::payment_init(TxnRequest &req) {
  payment_dep_.piece_warehouse = false;
  payment_dep_.piece_district = false;
  /**
   * req.input_
   *  0       ==> w_id, d_w_id
   *  1       ==> d_id
   *  2       ==> c_id or last_name
   *  3       ==> c_w_id
   *  4       ==> c_d_id
   *  5       ==> h_amount
   *  6       ==> h_key
   **/

  n_pieces_all_ = 6;
//  inputs_.resize(n_pieces_all_);
  output_size_.resize(n_pieces_all_);
  p_types_.resize(n_pieces_all_);
  sharding_.resize(n_pieces_all_);
  status_.resize(n_pieces_all_);

  // piece 0, Ri
  inputs_[0] = {
      {TPCC_VAR_W_ID, req.input_[0]},
      {TPCC_VAR_H_AMOUNT, req.input_[5]}
  };
  output_size_[0] = 6;
  p_types_[0] = TPCC_PAYMENT_0;
  payment_shard(TPCC_TB_WAREHOUSE, req.input_, sharding_[0]);
  status_[0] = READY;

  // piece 1, Ri district
  inputs_[1] = {
      {TPCC_VAR_W_ID, req.input_[0]},  // 0 ==>    d_w_id
      {TPCC_VAR_D_ID, req.input_[1]},  // 1 ==>    d_id
  };
  output_size_[1] = 6;
  p_types_[1] = TPCC_PAYMENT_1;
  payment_shard(TPCC_TB_DISTRICT, req.input_, sharding_[1]);
  status_[1] = READY;

  // piece 2, W district
  inputs_[2] = {
      {TPCC_VAR_W_ID,     req.input_[0]}, // 0 ==>    d_w_id
      {TPCC_VAR_D_ID,     req.input_[1]}, // 1 ==>    d_id
      {TPCC_VAR_H_AMOUNT, req.input_[5]}  // 2 ==>    h_amount
  };
  output_size_[2] = 0;
  p_types_[2] = TPCC_PAYMENT_2;
  payment_shard(TPCC_TB_DISTRICT, req.input_, sharding_[2]);
  status_[2] = READY;

  // query by c_last
  if (req.input_[2].get_kind() == mdb::Value::STR) {
    // piece 3, R customer, c_last -> c_id
    inputs_[3] = {
        {TPCC_VAR_C_LAST, req.input_[2]},  // 0 ==>    c_last
        {TPCC_VAR_C_W_ID, req.input_[3]},  // 1 ==>    c_w_id
        {TPCC_VAR_C_D_ID, req.input_[4]},  // 2 ==>    c_d_id
    };
    output_size_[3] = 1;
    p_types_[3] = TPCC_PAYMENT_3;
    payment_shard(TPCC_TB_CUSTOMER, req.input_, sharding_[3]);
    status_[3] = READY;
    // piece 4, set it to waiting
    status_[4] = WAITING;

    payment_dep_.piece_last2id = false;
    payment_dep_.piece_ori_last2id = false;
  }
    // query by c_id, invalidate piece 2
  else {
    Log_debug("payment transaction lookup by customer name");
//    verify(0);
    // piece 3, R customer, set it to finish
    status_[3] = FINISHED;
    // piece 4, set it to ready
    status_[4] = READY;
    n_pieces_out_ = 1;

    payment_dep_.piece_last2id = true;
    payment_dep_.piece_ori_last2id = true;
  }

  // piece 4, R & W customer
  inputs_[4] = {
      {TPCC_VAR_C_ID,     req.input_[2]},  // 0 ==>    c_id
      {TPCC_VAR_C_W_ID,   req.input_[3]},  // 1 ==>    c_w_id
      {TPCC_VAR_C_D_ID,   req.input_[4]},  // 2 ==>    c_d_id
      {TPCC_VAR_H_AMOUNT, req.input_[5]},  // 3 ==>    h_amount
      {TPCC_VAR_W_ID,     req.input_[0]},  // 4 ==>    w_id
      {TPCC_VAR_D_ID,     req.input_[1]}   // 5 ==>    d_id
  };
  output_size_[4] = 15;
  p_types_[4] = TPCC_PAYMENT_4;
  payment_shard(TPCC_TB_CUSTOMER, req.input_, sharding_[4]);

  // piece 5, W history (insert), depends on piece 0, 1
  inputs_[5] = {
      {TPCC_VAR_H_KEY,    req.input_[6]},  // 0 ==>    h_key
      {TPCC_VAR_W_NAME,   Value()},        // 1 ==>    w_name depends on piece 0
      {TPCC_VAR_D_NAME,   Value()},        // 2 ==>    d_name depends on piece 1
      {TPCC_VAR_W_ID,     req.input_[0]},  // 3 ==>    w_id
      {TPCC_VAR_D_ID,     req.input_[1]},  // 4 ==>    d_id
      {TPCC_VAR_C_ID,     req.input_[2]},  // 5 ==>    c_id depends on piece 2 if querying by c_last
      {TPCC_VAR_C_W_ID,   req.input_[3]},  // 6 ==>    c_w_id
      {TPCC_VAR_C_D_ID,   req.input_[4]},  // 7 ==>    c_d_id
      {TPCC_VAR_H_AMOUNT, req.input_[5]}   // 8 ==>    h_amount
  };
  output_size_[5] = 0;
  p_types_[5] = TPCC_PAYMENT_5;
  payment_shard(TPCC_TB_HISTORY, req.input_, sharding_[5]);
  status_[5] = WAITING;
}


void TpccChopper::payment_shard(const char *tb,
                                const std::vector<mdb::Value> &input,
                                uint32_t &site) {
  MultiValue mv;
  if (tb == TPCC_TB_WAREHOUSE || tb == TPCC_TB_DISTRICT)
    mv = MultiValue(input[0]);
  else if (tb == TPCC_TB_CUSTOMER)
    mv = MultiValue(input[3]);
  else if (tb == TPCC_TB_HISTORY)
    mv = MultiValue(input[6]);
  else
    verify(0);
  int ret = sss_->get_site_id_from_tb(tb, mv, site);
  verify(ret == 0);
}


void TpccChopper::payment_retry() {
  status_[0] = READY;
  status_[1] = READY;
  status_[2] = READY;
  // query by c_id
  if (payment_dep_.piece_ori_last2id) {
    status_[3] = FINISHED;
    status_[4] = READY;
    n_pieces_out_ = 1;
  }
    // query by c_last
  else {
    status_[3] = READY;
    status_[4] = WAITING;
  }
  status_[5] = WAITING;
  payment_dep_.piece_warehouse = false;
  payment_dep_.piece_district = false;
  payment_dep_.piece_last2id = payment_dep_.piece_ori_last2id;
}

void TpccPiece::reg_payment() {
  BEGIN_PIE(TPCC_PAYMENT,      // txn
          TPCC_PAYMENT_0,    // piece 0, Ri & W warehouse
          DF_NO) { // immediately read
    verify(input.size() == 2);
    Log_debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_0);
    i32 oi = 0;
    mdb::Row *r = dtxn->Query(dtxn->GetTable(TPCC_TB_WAREHOUSE),
                              input[TPCC_VAR_W_ID].get_blob(),
                              ROW_WAREHOUSE);
    // R warehouse
    dtxn->ReadColumn(r,
                     TPCC_COL_WAREHOUSE_W_NAME,
                     &output[TPCC_VAR_W_NAME]);
    dtxn->ReadColumn(r,
                     TPCC_COL_WAREHOUSE_W_STREET_1,
                     &output[TPCC_VAR_W_STREET_1]);
    dtxn->ReadColumn(r,
                     TPCC_COL_WAREHOUSE_W_STREET_2,
                     &output[TPCC_VAR_W_STREET_2]);
    dtxn->ReadColumn(r,
                     TPCC_COL_WAREHOUSE_W_CITY,
                     &output[TPCC_VAR_W_CITY]);
    dtxn->ReadColumn(r,
                     TPCC_COL_WAREHOUSE_W_STATE,
                     &output[TPCC_VAR_W_STATE]);
    dtxn->ReadColumn(r,
                     TPCC_COL_WAREHOUSE_W_ZIP,
                     &output[TPCC_VAR_W_ZIP]);
    *res = SUCCESS;
  } END_PIE

  BEGIN_CB(TPCC_PAYMENT, 0)
    TpccChopper *tpcc_ch = (TpccChopper*)ch;
    verify(!tpcc_ch->payment_dep_.piece_warehouse);
    verify(output.size() == 6);
    tpcc_ch->payment_dep_.piece_warehouse = true;
    tpcc_ch->inputs_[5][TPCC_VAR_W_NAME] = output[TPCC_VAR_W_NAME];
    if (tpcc_ch->payment_dep_.piece_district &&
        tpcc_ch->payment_dep_.piece_last2id) {
      Log_debug("warehouse: d_name c_id ready");
      tpcc_ch->status_[5] = READY;
      return true;
    }
    Log_debug("warehouse: d_name c_id not ready");
    return false;
  END_CB

  BEGIN_PIE(TPCC_PAYMENT,      // txn
          TPCC_PAYMENT_1,    // piece 1, Ri district
          DF_NO) { // immediately read
    verify(input.size() == 2);
    Log_debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_1);
    Value buf;
    mdb::MultiBlob mb(2);
    mb[0] = input[TPCC_VAR_D_ID].get_blob();
    mb[1] = input[TPCC_VAR_W_ID].get_blob();
    mdb::Row *r = dtxn->Query(dtxn->GetTable(TPCC_TB_DISTRICT),
                              mb,
                              ROW_DISTRICT);
    // R district
    dtxn->ReadColumn(r,
                     TPCC_COL_DISTRICT_D_NAME,
                     &output[TPCC_VAR_D_NAME]);
    dtxn->ReadColumn(r,
                     TPCC_COL_DISTRICT_D_STREET_1,
                     &output[TPCC_VAR_D_STREET_1]);
    dtxn->ReadColumn(r,
                     TPCC_COL_DISTRICT_D_STREET_2,
                     &output[TPCC_VAR_D_STREET_2]);
    dtxn->ReadColumn(r,
                     TPCC_COL_DISTRICT_D_CITY,
                     &output[TPCC_VAR_D_CITY]);
    dtxn->ReadColumn(r,
                     TPCC_COL_DISTRICT_D_STATE,
                     &output[TPCC_VAR_D_STATE]);
    dtxn->ReadColumn(r,
                     TPCC_COL_DISTRICT_D_ZIP,
                     &output[TPCC_VAR_D_ZIP]);

    *res = SUCCESS;
  } END_PIE

  BEGIN_CB(TPCC_PAYMENT, 1)
    TpccChopper *tpcc_ch = (TpccChopper*)ch;
    verify(!tpcc_ch->payment_dep_.piece_district);
    verify(output.size() == 6);
    tpcc_ch->payment_dep_.piece_district = true;
    tpcc_ch->inputs_[5][TPCC_VAR_D_NAME] = output[TPCC_VAR_D_NAME];
    if (tpcc_ch->payment_dep_.piece_warehouse &&
        tpcc_ch->payment_dep_.piece_last2id) {
      Log_debug("warehouse: w_name c_id ready");
      tpcc_ch->status_[5] = READY;
      return true;
    }
    Log_debug("warehouse: w_name c_id not ready");
    return false;
  END_CB

  BEGIN_PIE(TPCC_PAYMENT,      // txn
          TPCC_PAYMENT_2,    // piece 1, Ri & W district
          DF_REAL) {
    verify(input.size() == 3);
    Log_debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_2);

    Value buf(0.0);
    mdb::Row *r = NULL;
    mdb::MultiBlob mb(2);
    //cell_locator_t cl(TPCC_TB_DISTRICT, 2);
    mb[0] = input[TPCC_VAR_D_ID].get_blob();
    mb[1] = input[TPCC_VAR_W_ID].get_blob();
    r = dtxn->Query(dtxn->GetTable(TPCC_TB_DISTRICT), mb, ROW_DISTRICT_TEMP);
    verify(r->schema_ != nullptr);
    dtxn->ReadColumn(r, TPCC_COL_DISTRICT_D_YTD, &buf, TXN_BYPASS);
    // W district
    buf.set_double(buf.get_double() + input[TPCC_VAR_H_AMOUNT].get_double());
    dtxn->WriteColumn(r, TPCC_COL_DISTRICT_D_YTD, buf, TXN_DEFERRED);
    *res = SUCCESS;
  } END_PIE

  BEGIN_PIE(TPCC_PAYMENT,      // txn
          TPCC_PAYMENT_3,    // piece 2, R customer secondary index, c_last -> c_id
          DF_NO) {
    verify(input.size() == 3);
    Log_debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_3);

    mdb::MultiBlob mbl(3), mbh(3);
    mbl[0] = input[TPCC_VAR_C_D_ID].get_blob();
    mbh[0] = input[TPCC_VAR_C_D_ID].get_blob();
    mbl[1] = input[TPCC_VAR_C_W_ID].get_blob();
    mbh[1] = input[TPCC_VAR_C_W_ID].get_blob();
    Value c_id_low(std::numeric_limits<i32>::min());
    Value c_id_high(std::numeric_limits<i32>::max());
    mbl[2] = c_id_low.get_blob();
    mbh[2] = c_id_high.get_blob();

    c_last_id_t key_low(input[TPCC_VAR_C_LAST].get_str(), mbl, &C_LAST_SCHEMA);
    c_last_id_t key_high(input[TPCC_VAR_C_LAST].get_str(), mbh, &C_LAST_SCHEMA);
    std::multimap<c_last_id_t, rrr::i32>::iterator it, it_low, it_high, it_mid;
    bool inc = false, mid_set = false;
    it_low = C_LAST2ID.lower_bound(key_low);
    it_high = C_LAST2ID.upper_bound(key_high);
    int n_c = 0;
    for (it = it_low; it != it_high; it++) {
      n_c++;
      if (mid_set)
        if (inc) {
            it_mid++;
            inc = false;
        } else
            inc = true;
      else {
            mid_set = true;
            it_mid = it;
      }
    }
    Log_debug("w_id: %d, d_id: %d, c_last: %s, num customer: %d",
              input[TPCC_VAR_C_W_ID].get_i32(),
              input[TPCC_VAR_C_D_ID].get_i32(),
              input[TPCC_VAR_C_LAST].get_str().c_str(),
              n_c);
    verify(mid_set);
    output[TPCC_VAR_C_ID] = Value(it_mid->second);

    *res = SUCCESS;
  } END_PIE

  BEGIN_CB(TPCC_PAYMENT, 3)
    TpccChopper *tpcc_ch = (TpccChopper*)ch;
    verify(!tpcc_ch->payment_dep_.piece_last2id);
    verify(output.size() == 1);
    tpcc_ch->payment_dep_.piece_last2id = true;
    // set piece 4 ready
    tpcc_ch->inputs_[4][TPCC_VAR_C_ID] = output[TPCC_VAR_C_ID];
    tpcc_ch->status_[4] = READY;

    tpcc_ch->inputs_[5][TPCC_VAR_C_ID] = output[TPCC_VAR_C_ID];
    if (tpcc_ch->payment_dep_.piece_warehouse &&
        tpcc_ch->payment_dep_.piece_district) {
      tpcc_ch->status_[5] = READY;
      Log_debug("customer: w_name c_id not ready");
    }
    return true;
  END_CB

  BEGIN_PIE(TPCC_PAYMENT,      // txn
          TPCC_PAYMENT_4,    // piece 4, R & W customer
          DF_REAL) {
    verify(input.size() == 6);
    Log_debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_4);
    mdb::Row *r = NULL;
    mdb::MultiBlob mb(3);
    //cell_locator_t cl(TPCC_TB_CUSTOMER, 3);
    mb[0] = input[TPCC_VAR_C_ID].get_blob();
    mb[1] = input[TPCC_VAR_C_D_ID].get_blob();
    mb[2] = input[TPCC_VAR_C_W_ID].get_blob();
    // R customer
    r = dtxn->Query(dtxn->GetTable(TPCC_TB_CUSTOMER), mb, ROW_CUSTOMER);
    ALock::type_t lock_20_type = ALock::RLOCK;
    if (input[TPCC_VAR_C_ID].get_i32() % 10 == 0)
        lock_20_type = ALock::WLOCK;

    vector<Value> buf({
        Value(""), Value(""), Value(""), Value(""),
        Value(""), Value(""), Value(""), Value(""),
        Value(""), Value(""), Value(""), Value(""),
        Value(""), Value(0.0), Value(0.0), Value("")}
    );
    int oi = 0;
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_FIRST      , &buf[0] , TXN_BYPASS );
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_MIDDLE     , &buf[1] , TXN_BYPASS );
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_LAST       , &buf[2] , TXN_BYPASS );
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_STREET_1   , &buf[3] , TXN_BYPASS );
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_STREET_2   , &buf[4] , TXN_BYPASS );
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_CITY       , &buf[5] , TXN_BYPASS );
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_STATE      , &buf[6] , TXN_BYPASS );
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_ZIP        , &buf[7] , TXN_BYPASS );
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_PHONE      , &buf[8] , TXN_BYPASS );
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_SINCE      , &buf[9] , TXN_BYPASS );
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_CREDIT     , &buf[10], TXN_BYPASS );
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_CREDIT_LIM , &buf[11], TXN_BYPASS );
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_DISCOUNT   , &buf[12], TXN_BYPASS );
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_BALANCE    , &buf[13], TXN_DEFERRED);
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_YTD_PAYMENT, &buf[14], TXN_DEFERRED);
    dtxn->ReadColumn(r, TPCC_COL_CUSTOMER_C_DATA       , &buf[15], TXN_DEFERRED);

    // if c_credit == "BC" (bad) 10%
    // here we use c_id to pick up 10% instead of c_credit
    if (input[TPCC_VAR_C_ID].get_i32() % 10 == 0) {
      Value c_data((
              to_string(input[TPCC_VAR_C_ID])
                      + to_string(input[TPCC_VAR_C_D_ID])
                      + to_string(input[TPCC_VAR_C_W_ID])
                      + to_string(input[TPCC_VAR_D_ID])
                      + to_string(input[TPCC_VAR_W_ID])
                      + to_string(input[TPCC_VAR_H_AMOUNT])
                      + buf[15].get_str()
      ).substr(0, 500));
      std::vector<mdb::column_id_t> col_ids = {
          TPCC_COL_CUSTOMER_C_BALANCE,
          TPCC_COL_CUSTOMER_C_YTD_PAYMENT,
          TPCC_COL_CUSTOMER_C_DATA
      };
      std::vector<Value> col_data({
              Value(buf[13].get_double() - input[TPCC_VAR_H_AMOUNT].get_double()),
              Value(buf[14].get_double() + input[TPCC_VAR_H_AMOUNT].get_double()),
              c_data
      });
      dtxn->WriteColumns(r, col_ids, col_data);
    } else {
      std::vector<mdb::column_id_t> col_ids({
              TPCC_COL_CUSTOMER_C_BALANCE,
              TPCC_COL_CUSTOMER_C_YTD_PAYMENT
      });
      std::vector<Value> col_data({
              Value(buf[13].get_double() - input[TPCC_VAR_H_AMOUNT].get_double()),
              Value(buf[14].get_double() + input[TPCC_VAR_H_AMOUNT].get_double())
      });
      dtxn->WriteColumns(r, col_ids, col_data);
    }

    output[TPCC_VAR_C_ID] = input[TPCC_VAR_D_ID];  // output[0]  =>  c_id
    output[TPCC_VAR_C_FIRST] = buf[0];    // output[1]  =>  c_first
    output[TPCC_VAR_C_MIDDLE] = buf[1];    // output[2]  =>  c_middle
    output[TPCC_VAR_C_LAST] = buf[2];    // output[3]  =>  c_last
    output[TPCC_VAR_C_STREET_1] = buf[3];    // output[4]  =>  c_street_1
    output[TPCC_VAR_C_STREET_2] = buf[4];    // output[5]  =>  c_street_2
    output[TPCC_VAR_C_CITY] = buf[5];    // output[6]  =>  c_city
    output[TPCC_VAR_C_STATE] = buf[6];    // output[7]  =>  c_state
    output[TPCC_VAR_C_ZIP] = buf[7];    // output[8]  =>  c_zip
    output[TPCC_VAR_C_PHONE] = buf[8];    // output[9]  =>  c_phone
    output[TPCC_VAR_C_SINCE] = buf[9];    // output[10] =>  c_since
    output[TPCC_VAR_C_CREDIT] = buf[10];   // output[11] =>  c_credit
    output[TPCC_VAR_C_CREDIT_LIM] = buf[11];   // output[12] =>  c_credit_lim
    output[TPCC_VAR_C_DISCOUNT] = buf[12];   // output[13] =>  c_discount
    output[TPCC_VAR_C_BALANCE] = Value(buf[13].get_double() -
        input[TPCC_VAR_H_AMOUNT]
        .get_double()); // output[14] =>  c_balance

    *res = SUCCESS;
  } END_PIE

  BEGIN_CB(TPCC_PAYMENT, 4)
    TpccChopper *tpcc_ch = (TpccChopper*)ch;
    verify(output.size() == 15);
    if (!tpcc_ch->payment_dep_.piece_ori_last2id) {
      verify(output[TPCC_VAR_C_LAST].get_str() ==
          tpcc_ch->inputs_[3][TPCC_VAR_C_LAST].get_str());
    }
    return false;
  END_CB

  BEGIN_PIE(TPCC_PAYMENT,      // txn
          TPCC_PAYMENT_5,    // piece 4, W histroy
          DF_REAL) {
    verify(input.size() == 9);
    Log_debug("TPCC_PAYMENT, piece: %d", TPCC_PAYMENT_5);

    mdb::Txn *txn = dtxn->mdb_txn_;
    mdb::Table *tbl = txn->get_table(TPCC_TB_HISTORY);

    // insert history
    mdb::Row *r = NULL;

    std::vector<Value> row_data(9);
    row_data[0] = input[TPCC_VAR_H_KEY];              // h_key
    row_data[1] = input[TPCC_VAR_C_ID];               // h_c_id   =>  c_id
    row_data[2] = input[TPCC_VAR_C_D_ID];             // h_c_d_id =>  c_d_id
    row_data[3] = input[TPCC_VAR_C_W_ID];             // h_c_w_id =>  c_w_id
    row_data[4] = input[TPCC_VAR_C_D_ID];             // h_d_id   =>  d_id
    row_data[5] = input[TPCC_VAR_W_ID];               // h_d_w_id =>  d_w_id
    row_data[6] = Value(std::to_string(time(NULL)));  // h_date
    row_data[7] = input[TPCC_VAR_H_AMOUNT];           // h_amount =>  h_amount
    row_data[8] = Value(input[TPCC_VAR_W_NAME].get_str() +
                        "    " +
                        input[TPCC_VAR_D_NAME].get_str()); // d_data => w_name + 4spaces + d_name

    CREATE_ROW(tbl->schema(), row_data);

    txn->insert_row(tbl, r);
    *res = SUCCESS;
  } END_PIE
}

} // namespace rococo
