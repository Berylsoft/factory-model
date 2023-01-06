#![allow(dead_code, unused_imports)]

use std::{collections::BTreeMap, time::Duration, future::Future};
use async_channel::{Sender as Tx, Receiver as Rx, unbounded as channel};
use async_oneshot::{Sender as OneTx, Receiver as OneRx, oneshot};
use async_io::Timer;
use async_global_executor::{spawn, block_on};

type DealNo = u64;
type PartNo = u64;

#[derive(Debug, Clone)]
struct RawAtom;

#[derive(Debug, Clone)]
struct DerivedAtom;

#[derive(Debug, Clone)]
struct WithDealNo<T> {
    deal: DealNo,
    data: T,
}

type RawData = Vec<(PartNo, RawAtom)>;

#[derive(Debug, Clone)]
enum CheckResult {
    Ok,
    Failed,
}

type DerivedData = Vec<(PartNo, DerivedAtom)>;

struct Factroy {
    input: Rx<WithDealNo<RawData>>,
    checker: Tx<(RawData, OneTx<CheckResult>)>,
    // checkers: BTreeMap<PartNo, Tx<(RawAtom, Tx<CheckResultInner>)>>,
    check_result: Tx<WithDealNo<CheckResult>>,
    raw_output: Tx<WithDealNo<RawData>>,
    all_deriver: Tx<(RawData, OneTx<DerivedData>)>,
    // derivers: BTreeMap<PartNo, Tx<(RawAtom, Tx<DerivedAtom>)>>,
    derive_output: Tx<WithDealNo<DerivedData>>
}

fn main() {
    block_on(async {
        println!("started main_loop");
        let (input_tx, input_rx) = channel();
        let (checker_tx, checker_rx) = channel();
        let (check_result_tx, check_result_rx) = channel();
        let (raw_output_tx, raw_output_rx) = channel();
        let (all_deriver_tx, all_deriver_rx) = channel();
        let (derived_output_tx, derived_output_rx) = channel();
        let _self = Factroy {
            input: input_rx,
            checker: checker_tx,
            check_result: check_result_tx,
            raw_output: raw_output_tx,
            all_deriver: all_deriver_tx,
            derive_output: derived_output_tx,
        };

        let _ = spawn(async move {
            println!("started input_endpoint_loop");
            let mut deal = 0;
            loop {
                let data = vec![(0, RawAtom)];
                let raw_data = WithDealNo { deal, data };
                println!("input: {:?}", raw_data);
                input_tx.send(raw_data).await.unwrap();
                deal += 1;
                Timer::after(Duration::from_secs(5)).await;
            }
        });

        let _ = spawn(async move {
            println!("started checker_loop");
            while let Ok((data, mut res_tx)) = checker_rx.recv().await {
                println!("checker recv: {:?}", data);
                let res = CheckResult::Ok;
                res_tx.send(res).unwrap();
            }
        });

        let _ = spawn(async move {
            println!("started all_deriver_loop");
            while let Ok((data, mut res_tx)) = all_deriver_rx.recv().await {
                println!("all deriver recv: {:?}", data);
                let res = vec![(0, DerivedAtom)];
                res_tx.send(res).unwrap();
            }
        });

        let _ = spawn(async move {
            println!("started check_result_loop");
            while let Ok(check_result) = check_result_rx.recv().await {
                println!("check result: {:?}", check_result);
            }
        });

        let _ = spawn(async move {
            println!("started raw_output_loop");
            while let Ok(raw_output) = raw_output_rx.recv().await {
                println!("raw output: {:?}", raw_output);
            }
        });

        let _ = spawn(async move {
            println!("started derived_output_loop");
            while let Ok(derived_output) = derived_output_rx.recv().await {
                println!("derived output: {:?}", derived_output);
            }
        });

        println!("start to process");
        while let Ok(input) = _self.input.recv().await {
            println!("recv input {:?}", input);
            let WithDealNo { deal, data } = input;
            let check_result = {
                let (res_tx, res_rx) = oneshot();
                _self.checker.send((data.clone(), res_tx)).await.unwrap();
                res_rx.await.unwrap()
            };
            _self.check_result.send(WithDealNo { deal, data: check_result.clone() }).await.unwrap();
            if let CheckResult::Failed = check_result {
                continue;
            }
            _self.raw_output.send(WithDealNo { deal, data: data.clone() }).await.unwrap();
            let derived = {
                let (res_tx, res_rx) = oneshot();
                _self.all_deriver.send((data.clone(), res_tx)).await.unwrap();
                res_rx.await.unwrap()
            };
            _self.derive_output.send(WithDealNo { deal, data: derived }).await.unwrap();
        }
    });
}
