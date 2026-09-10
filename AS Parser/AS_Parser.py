#!/usr/bin/env python3
import ipaddress
import logging
import concurrent.futures as cf

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_URL = "https://stat.ripe.net/data/announced-prefixes/data.json"
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 30
WORKERS = 10  

logging.basicConfig(level=logging.INFO,format="%(asctime)s [%(levelname)s] %(message)s",datefmt="%H:%M:%S",)
log = logging.getLogger(__name__)

ASN_LIST = {
    "Akamai": "AS20940",
    "Akamai 2": "AS18209",
    "Akamai 3": "AS24319",
    "Akamai 4": "AS25019",
    "Akamai 5": "AS26008",
    "Akamai 6": "AS31108",
    "Akamai 7": "AS34164",
    "Akamai 8": "AS49846",
    "Akamai 9": "AS17204",
    "Akamai 10": "AS213120",
    "Akamai 11": "AS393234",
    "Akamai 12": "AS16625",
    "Akamai 13": "AS393560",
    "Akamai 14": "AS12222",
    "Akamai 15": "AS33905",
    "Akamai 16": "AS21342",
    "Akamai 17": "AS32787",
    "Akamai 18": "AS35994",
    "Akamai 19": "AS12400",
    "Akamai 20": "AS15802",
    "Akamai Cloud (Linode)": "AS63949",
    "Amazon Gov Cloud": "AS8987",
    "Amazon.com, Inc.": "AS16509",
    "Amazon.com, Inc. 2": "AS14618",
    "Amazon.com, Inc. 3": "AS7224",
    "Arelion (fka. Telia Carrier)": "AS1299",
    "Aruba S.p.A.": "AS200185",
    "Aruba S.p.A. 2": "AS31034",
    "Aruba S.p.A. 3": "AS205727",
    "Aruba S.p.A. 4": "AS199653",
    "Aruba S.p.A. 5": "AS199883",
    "Aruba S.p.A. 6": "AS202613",
    "Aruba S.p.A. 7": "AS213224",
    "AT&T Enterprises": "AS7018",
    "Blizzard Entertainment, Inc.": "AS57976",
    "BunnyCDN": "AS200325",
    "CacheFly": "AS30081",
    "CDN77 (Datacamp)": "AS60068",
    "CDN77 2 (Datacamp)": "AS212238",
    "Claranet Limited": "AS8426",
    "Cloudflare": "AS13335",
    "Cloudflare 2": "AS14789",
    "Cloudflare 3": "AS132892",
    "Cloudflare 4": "AS395747",
    "Cloudflare 5": "AS209242",
    "Clouvider": "AS62240",
    "Cogent": "AS174",
    "CONECTA FIBRA LTDA": "AS263270",
    "Conecta Tecnologia LTDA": "AS52981",
    "Contabo": "AS51167",
    "Contabo 2": "AS141995",
    "Contabo 3": "AS40021",
    "CreaNova": "AS51765",
    "DATUM": "AS196646",
    "DATUM 2": "AS33438",
    "DigitalOcean": "AS14061",
    "DigitalOcean 2": "AS46652",
    "DigitalOcean 3": "AS393406",
    "DreamHost": "AS29873",
    "Edgecast Inc": "AS14210",
    "Fastly": "AS54113",
    "FDCSERVERS": "AS30058",
    "Fellowship": "AS46461",
    "Firstcolo": "AS44066",
    "Firstcolo 2": "AS31400",
    "Firstcolo 3": "AS203833",
    "FranTech (BuyVM)": "AS53667",
    "G-Core": "AS199524",
    "G-Core 2": "AS202422",
    "Ghosty Networks LLC": "AS205759",
    "GleSYS": "AS42708",
    "GlobeNet": "AS52320",
    "GoDaddy": "AS26496",
    "GoDaddy 2": "AS398101",
    "Google Fiber Inc": "AS16591",
    "Google LLC": "AS15169",
    "Google LLC (YouTube)": "AS36040",
    "Google LLC 2": "AS19527",
    "Google LLC 3": "AS396982",
    "GTHost": "AS63023",
    "GTT Americas": "AS260",
    "GTT Communications": "AS3257",
    "GTT Communications Netherlands B.V.": "AS5580",
    "Hetzner": "AS24940",
    "Hetzner 2": "AS213230",
    "Hetzner 3": "AS212317",
    "Hetzner 4": "AS215859",
    "Hosteur": "AS20773",
    "HostGator, BlueHost": "AS46606",
    "Hostinger": "AS47583",
    "Hostinger 2": "AS204915",
    "HostPapa, ColoCrossing": "AS36352",
    "Hurricane Electric": "AS6939",
    "i3D.net B.V": "AS49544",
    "INCAPSULA": "AS19551",
    "IOMART": "AS20860",
    "IOMART 2": "AS21130",
    "Ionos": "AS8560",
    "Ionos 2": "AS15418",
    "Itanet Conecta Ltda": "AS52699",
    "LWS SARL": "AS210403",
    "Level3 Carrier Ltd": "AS58682",
    "LogicForge": "AS208621",
    "Lumen (Level 3)": "AS3356",
    "M247 Europe SRL": "AS9009",
    "Majestic Hosting Solutions, LLC": "AS396073",
    "Melbicom": "AS8849",
    "Melbicom 2": "AS56630",
    "Meta": "AS32934",
    "Microsoft Corporation": "AS8068",
    "Microsoft Corporation 2": "AS8069",
    "Microsoft Corporation 3": "AS8070",
    "Microsoft Corporation 4": "AS8071",
    "Microsoft Corporation 5": "AS8072",
    "Microsoft Corporation 6": "AS8073",
    "Microsoft Corporation 7": "AS8074",
    "Microsoft Corporation 8": "AS8075",
    "Netfiber Conecta 2020 SLU": "AS202384",
    "NTT Global": "AS2914",
    "NTT Global 10": "AS7671",
    "NTT Global 2": "AS10217",
    "NTT Global 3": "AS203329",
    "NTT Global 4": "AS4713",
    "NTT Global 5": "AS29017",
    "NTT Global 6": "AS147168",
    "NTT Global 7": "AS11158",
    "NTT Global 8": "AS19893",
    "NTT Global 9": "AS10204",
    "Opentransit Orange S.A.": "AS5511",
    "Oracle 2": "AS6142",
    "Oracle 3": "AS20054",
    "Oracle 4": "AS54253",
    "Oracle Cloud": "AS31898",
    "OVH": "AS16276",
    "OVH 2": "AS35540",
    "Paratus Telecommunications Limited": "AS33763",
    "PROXAD Free SAS": "AS12322",
    "Rapidzone": "AS13334",
    "Reliance Jio Infocomm Limited": "AS55836",
    "Riot Games, Inc.": "AS6507",
    "Scaleway": "AS12876",
    "Scaleway 2": "AS29447",
    "Scalaxy": "AS58061",
    "Tata Communications": "AS6453",
    "Tata Communications 2": "AS4755",
    "Tata Communications 3": "AS10199",
    "Telegram": "AS62041",
    "TELECOM ITALIA SPARKLE": "AS6762",
    "TELECOM ITALIA SPARKLE 2": "AS133757",
    "Telia-Lietuva Telia Lietuva": "AS5522",
    "UUNET": "AS701",
    "UUNET 2": "AS702",
    "UUNET 3": "AS703",
    "UUNET 4": "AS704",
    "UUNET 5": "AS705",
    "Vorboss Limited": "AS25160",
    "Vultr (Constant)": "AS20473",
    "Zenlayer": "AS21859",
}

session = requests.Session()
retry = Retry(total=5, backoff_factor=1.5, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET",))
session.mount("https://", HTTPAdapter(max_retries=retry))

def fetch(name: str, asn: str) -> tuple[set, set]:
    v4, v6 = set(), set()

    try:
        r = session.get(API_URL,params={"resource": asn, "min_peers_seeing": 1},timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        r.raise_for_status()
        prefixes = r.json().get("data", {}).get("prefixes", [])
    except Exception as e:
        log.warning("%s (%s): ошибка — %s", name, asn, e)
        return v4, v6

    for p in prefixes:
        prefix = p.get("prefix")
        if not prefix:
            continue
        try:
            net = ipaddress.ip_network(prefix, strict=False)
        except ValueError:
            continue
        if net.prefixlen == 0 or not net.is_global:
            continue
        (v4 if net.version == 4 else v6).add(net)

    log.info("%s (%s): %d префиксов", name, asn, len(v4) + len(v6))
    return v4, v6


def main() -> None:
    log.info("Старт сбора для %d ASN (workers=%d)", len(ASN_LIST), WORKERS)
    v4_all, v6_all = set(), set()

    with cf.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(fetch, name, asn) for name, asn in ASN_LIST.items()]
        for future in cf.as_completed(futures):
            v4, v6 = future.result()
            v4_all |= v4
            v6_all |= v6

    v4_sorted = sorted(
        ipaddress.collapse_addresses(sorted(v4_all, key=lambda n: (int(n.network_address), n.prefixlen))),
        key=lambda n: (int(n.network_address), n.prefixlen),
    )
    v6_sorted = sorted(
        ipaddress.collapse_addresses(sorted(v6_all, key=lambda n: (int(n.network_address), n.prefixlen))),
        key=lambda n: (int(n.network_address), n.prefixlen),
    )

    with open("ipset-all.txt", "w", encoding="utf-8") as f:
        for net in v4_sorted:
            f.write(str(net) + "\n")
        for net in v6_sorted:
            f.write(str(net) + "\n")

    log.info(
        "Готово! IPv4: %d | IPv6: %d | Всего: %d",
        len(v4_sorted), len(v6_sorted), len(v4_sorted) + len(v6_sorted),
    )


if __name__ == "__main__":
    main()
