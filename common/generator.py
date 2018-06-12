#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence 
@file: generator.py
@time: 2016/12/13 17:03
"""
from task.search.cracker.gsxt_anhui_worker import GsxtAnHuiWorker
from task.search.cracker.gsxt_beijing_worker import GsxtBeiJingWorker
from task.search.cracker.gsxt_chongqing_worker import GsxtChongQingWorker
from task.search.cracker.gsxt_fujian_worker import GsxtFuJianWorker
from task.search.cracker.gsxt_gansu_worker import GsxtGanSuWorker
from task.search.cracker.gsxt_gsxt_worker import GsxtGsxtWorker
from task.search.cracker.gsxt_guangdong_worker import GsxtGuangDongWorker
from task.search.cracker.gsxt_guangxi_worker import GsxtGuangXiWorker
from task.search.cracker.gsxt_guizhou_worker import GsxtGuiZhouWorker
from task.search.cracker.gsxt_hainan_worker import GsxtHaiNanWorker
from task.search.cracker.gsxt_hebei_worker import GsxtHeBeiWorker
from task.search.cracker.gsxt_heilongjiang_worker import GsxtHeiLongJiangWorker
from task.search.cracker.gsxt_henan_worker import GsxtHeNanWorker
from task.search.cracker.gsxt_hubei_worker import GsxtHuBeiWorker
from task.search.cracker.gsxt_hunan_worker import GsxtHuNanWorker
from task.search.cracker.gsxt_jiangsu_worker import GsxtJiangSuWorker
from task.search.cracker.gsxt_jiangxi_worker import GsxtJiangXiWorker
from task.search.cracker.gsxt_jilin_worker import GsxtJiLinWorker
from task.search.cracker.gsxt_neimenggu_worker import GsxtNeiMengGuWorker
from task.search.cracker.gsxt_ningxia_worker import GsxtNingXiaWorker
from task.search.cracker.gsxt_qinghai_worker import GsxtQingHaiWorker
from task.search.cracker.gsxt_shanghai_worker import GsxtShangHaiWorker
from task.search.cracker.gsxt_shanxi_worker import GsxtShanXiWorker
from task.search.cracker.gsxt_shanxicu_worker import GsxtShanXiCuWorker
from task.search.cracker.gsxt_sichuan_worker import GsxtSiChuanWorker
from task.search.cracker.gsxt_tianjin_worker import GsxtTianJinWorker
from task.search.cracker.gsxt_xinjiang_worker import GsxtXinJiangWorker
from task.search.cracker.gsxt_xizang_worker import GsxtXiZangWorker
from task.search.cracker.gsxt_yunnan_worker import GsxtYunNanWorker
from task.search.cracker.gsxt_zhejiang_worker import GsxtZheJiangWorker
from task.search.exploit.gsxt_liaoning_worker import GsxtLiaoNingWorker
from task.search.exploit.gsxt_shandong_worker import GsxtShanDongWorker
from task.split.detail.gsxt_detail_anhui_worker import GsxtDetailAnHuiWorker
from task.split.detail.gsxt_detail_beijing_worker import GsxtDetailBeiJingWorker
from task.split.detail.gsxt_detail_chongqing_worker import GsxtDetailChongQingWorker
from task.split.detail.gsxt_detail_fujian_worker import GsxtDetailFuJianWorker
from task.split.detail.gsxt_detail_gansu_worker import GsxtDetailGanSuWorker
from task.split.detail.gsxt_detail_gsxt_worker import GsxtDetailGsxtWorker
from task.split.detail.gsxt_detail_guangdong_worker import GsxtDetailGuangDongWorker
from task.split.detail.gsxt_detail_guangxi_worker import GsxtDetailGuangXiWorker
from task.split.detail.gsxt_detail_guizhou_worker import GsxtDetailGuiZhouWorker
from task.split.detail.gsxt_detail_hainan_worker import GsxtDetailHaiNanWorker
from task.split.detail.gsxt_detail_hebei_worker import GsxtDetailHeBeiWorker
from task.split.detail.gsxt_detail_heilongjiang_worker import GsxtDetailHeiLongJiangWorker
from task.split.detail.gsxt_detail_henan_worker import GsxtDetailHeNanWorker
from task.split.detail.gsxt_detail_hubei_worker import GsxtDetailHuBeiWorker
from task.split.detail.gsxt_detail_hunan_worker import GsxtDetailHuNanWorker
from task.split.detail.gsxt_detail_jiangsu_worker import GsxtDetailJiangSuWorker
from task.split.detail.gsxt_detail_jiangxi_worker import GsxtDetailJiangXiWorker
from task.split.detail.gsxt_detail_jilin_worker import GsxtDetailJiLinWorker
from task.split.detail.gsxt_detail_liaoning_worker import GsxtDetailLiaoNingWorker
from task.split.detail.gsxt_detail_neimenggu_worker import GsxtDetailNeiMengGuWorker
from task.split.detail.gsxt_detail_ningxia_worker import GsxtDetailNingXiaWorker
from task.split.detail.gsxt_detail_qinghai_worker import GsxtDetailQingHaiWorker
from task.split.detail.gsxt_detail_shandong_worker import GsxtDetailShanDongWorker
from task.split.detail.gsxt_detail_shanghai_worker import GsxtDetailShangHaiWorker
from task.split.detail.gsxt_detail_shanxi_worker import GsxtDetailShanXiWorker
from task.split.detail.gsxt_detail_shanxicu_worker import GsxtDetailShanXiCuWorker
from task.split.detail.gsxt_detail_sichuan_worker import GsxtDetailSiChuanWorker
from task.split.detail.gsxt_detail_tianjin_worker import GsxtDetailTianJinWorker
from task.split.detail.gsxt_detail_xinjiang_worker import GsxtDetailXinJiangWorker
from task.split.detail.gsxt_detail_xizang_worker import GsxtDetailXiZangWorker
from task.split.detail.gsxt_detail_yunnan_worker import GsxtDetailYunNanWorker
from task.split.detail.gsxt_detail_zhejiang_worker import GsxtDetailZheJiangWorker
from task.split.searchlist.gsxt_searchlist_anhui_worker import GsxtSearchListAnHuiWorker
from task.split.searchlist.gsxt_searchlist_beijing_worker import GsxtSearchListBeiJingWorker
from task.split.searchlist.gsxt_searchlist_chongqing_worker import GsxtSearchListChongQingWorker
from task.split.searchlist.gsxt_searchlist_fujian_worker import GsxtSearchListFuJianWorker
from task.split.searchlist.gsxt_searchlist_gansu_worker import GsxtSearchListGanSuWorker
from task.split.searchlist.gsxt_searchlist_gsxt_worker import GsxtSearchListGsxtWorker
from task.split.searchlist.gsxt_searchlist_guangdong_worker import GsxtSearchListGuangDongWorker
from task.split.searchlist.gsxt_searchlist_guangxi_worker import GsxtSearchListGuangXiWorker
from task.split.searchlist.gsxt_searchlist_guizhou_worker import GsxtSearchListGuiZhouWorker
from task.split.searchlist.gsxt_searchlist_hainan_worker import GsxtSearchListHaiNanWorker
from task.split.searchlist.gsxt_searchlist_hebei_worker import GsxtSearchListHeBeiWorker
from task.split.searchlist.gsxt_searchlist_heilongjiang_worker import GsxtSearchListHeiLongJiangWorker
from task.split.searchlist.gsxt_searchlist_henan_worker import GsxtSearchListHeNanWorker
from task.split.searchlist.gsxt_searchlist_hubei_worker import GsxtSearchListHuBeiWorker
from task.split.searchlist.gsxt_searchlist_hunan_worker import GsxtSearchListHuNanWorker
from task.split.searchlist.gsxt_searchlist_jiangsu_worker import GsxtSearchListJiangSuWorker
from task.split.searchlist.gsxt_searchlist_jiangxi_worker import GsxtSearchListJiangXiWorker
from task.split.searchlist.gsxt_searchlist_jilin_worker import GsxtSearchListJiLinWorker
from task.split.searchlist.gsxt_searchlist_liaoning_worker import GsxtSearchListLiaoNingWorker
from task.split.searchlist.gsxt_searchlist_neimenggu_worker import GsxtSearchListNeiMengGuWorker
from task.split.searchlist.gsxt_searchlist_ningxia_worker import GsxtSearchListNingXiaWorker
from task.split.searchlist.gsxt_searchlist_qinghai_worker import GsxtSearchListQingHaiWorker
from task.split.searchlist.gsxt_searchlist_shandong_worker import GsxtSearchListShanDongWorker
from task.split.searchlist.gsxt_searchlist_shanghai_worker import GsxtSearchListShangHaiWorker
from task.split.searchlist.gsxt_searchlist_shanxi_worker import GsxtSearchListShanXiWorker
from task.split.searchlist.gsxt_searchlist_shanxicu_worker import GsxtSearchListShanXiCuWorker
from task.split.searchlist.gsxt_searchlist_sichuan_worker import GsxtSearchListSiChuanWorker
from task.split.searchlist.gsxt_searchlist_tianjin_worker import GsxtSearchListTianJinWorker
from task.split.searchlist.gsxt_searchlist_xinjiang_worker import GsxtSearchListXinJiangWorker
from task.split.searchlist.gsxt_searchlist_xizang_worker import GsxtSearchListXiZangWorker
from task.split.searchlist.gsxt_searchlist_yunnan_worker import GsxtSearchListYunNanWorker
from task.split.searchlist.gsxt_searchlist_zhejiang_worker import GsxtSearchListZheJiangWorker

__all__ = ['GsxtHuNanWorker', 'GsxtShanXiWorker', 'GsxtNingXiaWorker', 'GsxtGuiZhouWorker',
           'GsxtLiaoNingWorker', 'GsxtJiangSuWorker', 'GsxtNeiMengGuWorker', 'GsxtTianJinWorker',
           'GsxtShanXiCuWorker', 'GsxtChongQingWorker', 'GsxtSiChuanWorker', 'GsxtXinJiangWorker',
           'GsxtShangHaiWorker', 'GsxtFuJianWorker', 'GsxtGuangDongWorker', 'GsxtBeiJingWorker',
           'GsxtHeiLongJiangWorker', 'GsxtAnHuiWorker', 'GsxtShanDongWorker', 'GsxtGuangXiWorker',
           'GsxtXiZangWorker', 'GsxtQingHaiWorker', 'GsxtHuBeiWorker', 'GsxtHeNanWorker',
           'GsxtHaiNanWorker', 'GsxtGanSuWorker',
           'GsxtDetailAnHuiWorker',
           'GsxtDetailLiaoNingWorker', 'GsxtDetailShanXiCuWorker', 'GsxtDetailShanDongWorker',
           'GsxtDetailBeiJingWorker', 'GsxtDetailXiZangWorker', 'GsxtDetailQingHaiWorker',
           'GsxtDetailHeiLongJiangWorker', 'GsxtDetailHeNanWorker', 'GsxtDetailHuBeiWorker',
           'GsxtHeBeiWorker', 'GsxtYunNanWorker', 'GsxtSearchListBeiJingWorker', 'GsxtSearchListShangHaiWorker',
           'GsxtSearchListGuangDongWorker', 'GsxtSearchListAnHuiWorker', 'GsxtDetailShangHaiWorker',
           'GsxtDetailGuangDongWorker',
           'GsxtSearchListLiaoNingWorker', 'GsxtSearchListShanDongWorker', 'GsxtSearchListShanXiCuWorker',
           'GsxtSearchListFuJianWorker', 'GsxtDetailFuJianWorker', 'GsxtSearchListHeNanWorker',
           'GsxtDetailHuNanWorker', 'GsxtSearchListHuNanWorker', 'GsxtSearchListHuBeiWorker',
           'GsxtSearchListTianJinWorker', 'GsxtSearchListHeBeiWorker', 'GsxtSearchListNeiMengGuWorker',
           'GsxtJiLinWorker', 'GsxtSearchListJiLinWorker', 'GsxtSearchListHeiLongJiangWorker',
           'GsxtSearchListJiangSuWorker', 'GsxtZheJiangWorker', 'GsxtSearchListZheJiangWorker',
           'GsxtJiangXiWorker', 'GsxtSearchListGuangXiWorker', 'GsxtSearchListHaiNanWorker',
           'GsxtSearchListJiangXiWorker', 'GsxtSearchListChongQingWorker', 'GsxtSearchListSiChuanWorker',
           'GsxtSearchListGuiZhouWorker', 'GsxtSearchListYunNanWorker', 'GsxtSearchListXiZangWorker',
           'GsxtSearchListShanXiWorker', 'GsxtSearchListGanSuWorker', 'GsxtSearchListQingHaiWorker',
           'GsxtSearchListNingXiaWorker', 'GsxtSearchListXinJiangWorker', 'GsxtDetailJiangSuWorker',
           'GsxtDetailZheJiangWorker', 'GsxtDetailSiChuanWorker', 'GsxtDetailJiangXiWorker',
           'GsxtDetailTianJinWorker', 'GsxtDetailShanXiWorker', 'GsxtDetailChongQingWorker',
           'GsxtDetailYunNanWorker', 'GsxtDetailHeBeiWorker', 'GsxtDetailXinJiangWorker',
           'GsxtDetailGuiZhouWorker', 'GsxtDetailGuangXiWorker', 'GsxtDetailHaiNanWorker',
           'GsxtDetailGanSuWorker', 'GsxtDetailJiLinWorker', 'GsxtDetailNeiMengGuWorker',
           'GsxtDetailNingXiaWorker', 'CheckAnHuiWorker', 'CheckBeiJingWorker', 'CheckChongQingWorker',
           'CheckFuJianWorker', 'CheckGanSuWorker', 'CheckGuangDongWorker', 'CheckGuangXiWorker',
           'CheckGuiZhouWorker', 'CheckHaiNanWorker', 'CheckHeBeiWorker', 'CheckHeiLongJiangWorker',
           'CheckHeNanWorker', 'CheckHuBeiWorker', 'CheckHuNanWorker', 'CheckJiangSuWorker',
           'CheckJiangXiWorker', 'CheckJiLinWorker', 'CheckLiaoNingWorker', 'CheckNeiMengGuWorker',
           'CheckNingXiaWorker', 'CheckQingHaiWorker', 'CheckShanDongWorker', 'CheckShangHaiWorker',
           'CheckShanXiWorker', 'CheckShanXiCuWorker', 'CheckSiChuanWorker', 'CheckTianJinWorker',
           'CheckXinJiangWorker', 'CheckXiZangWorker', 'CheckYunNanWorker', 'CheckZheJiangWorker',
           'GsxtGsxtWorker', 'CheckGsxtWorker', 'GsxtDetailGsxtWorker', 'GsxtSearchListGsxtWorker',
           ]


def create_crawl_object(config_dict, province):
    clazz = config_dict.get('clazz', '')
    if clazz == '':
        raise StandardError("no worker for {province}".format(province=province))

    return eval(clazz)(**config_dict)
