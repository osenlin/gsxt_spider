#encoding=utf-8
import requests

def main():
    
    with open("proxies_200.txt") as pfile:
        for line in pfile:
            line = line.strip()
            print line
            json={
                "proxy":line,
                "searchBtnSelector": "#btnSearch",
                "searchText": u"数据科技有限",
                "searchInputSelector": "#txtSearch",
                "url": "http://jl.gsxt.gov.cn/",
                "successIndicatorSelector": ".m-search-list",
                "resultIndicatorSelector": ".m-searchresult-inoformation"
            }
            page=requests.post('http://sm5.sz-internal.haizhi.com:59876/api/crawl_scripts/gongshang',json = json)
            print page.status_code,page.content


if __name__ == "__main__":
    main()

