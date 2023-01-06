#![allow(dead_code)]

use std::{collections::btree_map::{BTreeMap, Entry as BTreeMapEntry}, time::Duration};
use async_channel::{Sender as Tx, Receiver as Rx, unbounded as channel};
use async_oneshot::{Sender as OneTx, oneshot};
use async_io::Timer;
use async_global_executor::{spawn, block_on};

async fn send_recv<Req, Res>(tx: &Tx<(Req, OneTx<Res>)>, req: Req) -> Res {
    let (res_tx, res_rx) = oneshot();
    tx.send((req, res_tx)).await.unwrap();
    res_rx.await.unwrap()
}

fn respond<Req, Res>((req, mut res_tx): (Req, OneTx<Res>), responder: fn(Req) -> Res) {
    res_tx.send(responder(req)).unwrap()
}

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

type CheckResult = Option<CheckError>;

#[derive(Debug, Clone)]
struct CheckError;

#[derive(Debug, Clone)]
struct CombinedCheckError {
    error: CheckError,
    pos: CheckErrorPos,
}

#[derive(Debug, Clone)]
enum CheckErrorPos {
    All,
    Part(PartNo),
}

fn combine_result(_all: Option<CheckError>, _parts: Vec<WithPartNo<Option<CheckError>>>) -> Option<CombinedCheckError> {
    None
}

type DerivedData = Vec<WithPartNo<DerivedAtom>>;

struct Factroy {
    input_rx: Rx<WithDealNo<RawData>>,
    checker_tx: Tx<(RawData, OneTx<CheckResult>)>,
    checkers_reg: Reg<CheckResult>,
    check_result_tx: Tx<WithDealNo<Option<CombinedCheckError>>>,
    raw_output_tx: Tx<WithDealNo<RawData>>,
    derivers_reg: Reg<DerivedAtom>,
    derived_output_tx: Tx<WithDealNo<DerivedData>>,
}

struct InitPorts {
    input_tx: Tx<WithDealNo<RawData>>,
    checker_rx: Rx<(RawData, OneTx<CheckResult>)>,
    check_result_rx: Rx<WithDealNo<Option<CombinedCheckError>>>,
    raw_output_rx: Rx<WithDealNo<RawData>>,
    derived_output_rx: Rx<WithDealNo<DerivedData>>,
}

struct Reg<T> {
    tx: BTreeMap<PartNo, Tx<(RawAtom, OneTx<T>)>>,
}

impl<T> Reg<T> {
    fn new() -> Reg<T> {
        Reg { tx: BTreeMap::new() }
    }

    fn reg(&mut self, part: PartNo) -> Rx<(RawAtom, OneTx<T>)> {
        match self.tx.entry(part) {
            BTreeMapEntry::Occupied(_) => unreachable!(),
            BTreeMapEntry::Vacant(entry) => {
                let (tx, rx) = channel();
                entry.insert(tx);
                rx
            },
        }
    }

    async fn send_recv_ordered(&self, raw: RawData) -> Vec<WithPartNo<T>> {
        let mut res = Vec::with_capacity(raw.len());
        for WithPartNo { part, data } in raw {
            res.push(WithPartNo { part, data: send_recv(self.tx.get(&part).unwrap(), data).await });
        }
        res
    }
}

impl Factroy {
    fn new() -> (Factroy, InitPorts) {
        let (input_tx, input_rx) = channel();
        let (checker_tx, checker_rx) = channel();
        let (check_result_tx, check_result_rx) = channel();
        let (raw_output_tx, raw_output_rx) = channel();
        let (derived_output_tx, derived_output_rx) = channel();

        let checkers_reg = Reg::new();
        let derivers_reg = Reg::new();

        (
            Factroy {
                input_rx,
                checker_tx,
                checkers_reg,
                check_result_tx,
                raw_output_tx,
                derivers_reg,
                derived_output_tx,
            },
            InitPorts {
                input_tx,
                checker_rx,
                check_result_rx,
                raw_output_rx,
                derived_output_rx,
            },
        )
    }

    async fn reg(&mut self) {
        let checker0 = self.checkers_reg.reg(0);
        spawn(async move {
            println!("started checker0_loop");
            while let Ok(req) = checker0.recv().await {
                println!("checker0 recv: {:?}", req.0);
                respond(req, |_| None)
            }
        }).detach();

        let deriver0 = self.derivers_reg.reg(0);
        spawn(async move {
            println!("started deriver0_loop");
            while let Ok(req) = deriver0.recv().await {
                println!("deriver0 recv: {:?}", req.0);
                respond(req, |_| DerivedAtom);
            }
        }).detach();
    }

    async fn exec(self) {
        while let Ok(input) = self.input_rx.recv().await {
            println!("recv input {:?}", input);
            let WithDealNo { deal, data } = input;
            let check_result_all = send_recv(&self.checker_tx, data.clone()).await;
            let check_result_parts = self.checkers_reg.send_recv_ordered(data.clone()).await;
            let check_result = combine_result(check_result_all, check_result_parts);
            self.check_result_tx.send(WithDealNo { deal, data: check_result.clone() }).await.unwrap();
            if let Some(_) = check_result { continue; }
            self.raw_output_tx.send(WithDealNo { deal, data: data.clone() }).await.unwrap();
            let derived = self.derivers_reg.send_recv_ordered(data.clone()).await;
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
            while let Ok(req) = self.checker_rx.recv().await {
                println!("checker recv: {:?}", req.0);
                respond(req, |_| None)
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
        let (mut factory, ports) = Factroy::new();
        ports.debug().await;
        println!("start to process");
        factory.reg().await;
        factory.exec().await;
    });
}
