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

#[derive(Debug, Clone)]
struct WithPartNo<T> {
    part: PartNo,
    data: T,
}

type RawData = Vec<WithPartNo<RawAtom>>;

#[derive(Debug, Clone)]
enum CheckResult {
    Ok,
    Failed,
}

type DerivedData = Vec<WithPartNo<DerivedAtom>>;

struct Factroy {
    input_rx: Rx<WithDealNo<RawData>>,
    checker_tx: Tx<(RawData, OneTx<CheckResult>)>,
    checkers: BTreeMap<PartNo, Tx<(RawAtom, Tx<CheckResult>)>>,
    check_result_tx: Tx<WithDealNo<CheckResult>>,
    raw_output_tx: Tx<WithDealNo<RawData>>,
    all_deriver_tx: Tx<(RawData, OneTx<DerivedData>)>,
    derivers: BTreeMap<PartNo, Tx<(RawAtom, Tx<DerivedAtom>)>>,
    derived_output_tx: Tx<WithDealNo<DerivedData>>
}

struct InitPorts {
    input_tx: Tx<WithDealNo<RawData>>,
    checker_rx: Rx<(RawData, OneTx<CheckResult>)>,
    all_deriver_rx: Rx<(RawData, OneTx<DerivedData>)>,
    check_result_rx: Rx<WithDealNo<CheckResult>>,
    raw_output_rx: Rx<WithDealNo<RawData>>,
    derived_output_rx: Rx<WithDealNo<DerivedData>>,
}

impl Factroy {
    fn new() -> (Factroy, InitPorts) {
        let (input_tx, input_rx) = channel();
        let (checker_tx, checker_rx) = channel();
        let (check_result_tx, check_result_rx) = channel();
        let (raw_output_tx, raw_output_rx) = channel();
        let (all_deriver_tx, all_deriver_rx) = channel();
        let (derived_output_tx, derived_output_rx) = channel();

        (
            Factroy {
                input_rx,
                checker_tx,
                checkers: BTreeMap::new(),
                check_result_tx,
                raw_output_tx,
                all_deriver_tx,
                derivers: BTreeMap::new(),
                derived_output_tx,
            },
            InitPorts {
                input_tx,
                checker_rx,
                check_result_rx,
                raw_output_rx,
                all_deriver_rx,
                derived_output_rx,
            },
        )
    }

    async fn exec(self) {
        while let Ok(input) = self.input_rx.recv().await {
            println!("recv input {:?}", input);
            let WithDealNo { deal, data } = input;
            let check_result = {
                let (res_tx, res_rx) = oneshot();
                self.checker_tx.send((data.clone(), res_tx)).await.unwrap();
                res_rx.await.unwrap()
            };
            self.check_result_tx.send(WithDealNo { deal, data: check_result.clone() }).await.unwrap();
            if let CheckResult::Failed = check_result {
                continue;
            }
            self.raw_output_tx.send(WithDealNo { deal, data: data.clone() }).await.unwrap();
            let derived = {
                let (res_tx, res_rx) = oneshot();
                self.all_deriver_tx.send((data.clone(), res_tx)).await.unwrap();
                res_rx.await.unwrap()
            };
            self.derived_output_tx.send(WithDealNo { deal, data: derived }).await.unwrap();
        }
    }
}

impl InitPorts {
    async fn debug(self) {
        spawn(async move {
            println!("started input_endpoint_loop");
            let mut deal = 0;
            loop {
                let data = vec![WithPartNo { part: 0, data: RawAtom }];
                let raw_data = WithDealNo { deal, data };
                println!("input: {:?}", raw_data);
                self.input_tx.send(raw_data).await.unwrap();
                deal += 1;
                Timer::after(Duration::from_secs(5)).await;
            }
        }).detach();

        spawn(async move {
            println!("started checker_loop");
            while let Ok((data, mut res_tx)) = self.checker_rx.recv().await {
                println!("checker recv: {:?}", data);
                let res = CheckResult::Ok;
                res_tx.send(res).unwrap();
            }
        }).detach();

        spawn(async move {
            println!("started all_deriver_loop");
            while let Ok((data, mut res_tx)) = self.all_deriver_rx.recv().await {
                println!("all deriver recv: {:?}", data);
                let res = vec![WithPartNo { part: 0, data: DerivedAtom }];
                res_tx.send(res).unwrap();
            }
        }).detach();

        spawn(async move {
            println!("started check_result_loop");
            while let Ok(check_result) = self.check_result_rx.recv().await {
                println!("check result: {:?}", check_result);
            }
        }).detach();

        spawn(async move {
            println!("started raw_output_loop");
            while let Ok(raw_output) = self.raw_output_rx.recv().await {
                println!("raw output: {:?}", raw_output);
            }
        }).detach();

        spawn(async move {
            println!("started derived_output_loop");
            while let Ok(derived_output) = self.derived_output_rx.recv().await {
                println!("derived output: {:?}", derived_output);
            }
        }).detach();
    }
}

fn main() {
    block_on(async {
        println!("started main_loop");
        let (factory, ports) = Factroy::new();
        ports.debug().await;
        println!("start to process");
        factory.exec().await;
    });
}
