# -*- coding: utf-8 -*-
import scrapy
from scrapy.selector import Selector
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.http import FormRequest
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
import csv
import threading


class SwuSpiderSpider(scrapy.Spider):
    name = 'swu_spider'
    allowed_domains = ['swachhbharaturban.gov.in/ihhl/RPTApplicationSummary.aspx']
    start_urls = ['http://swachhbharaturban.gov.in/ihhl/RPTApplicationSummary.aspx/']

    def __init__(self, data_dir_path='/home/user/swachchbharaturban/swu_spider/data', raw_dir_path='/home/user/swachchbharaturban/swu_spider/raw',  url=None, *args, **kwargs):
        super(SwuSpiderSpider, self).__init__(*args, **kwargs)
        self.data_dir_path = data_dir_path
        self.raw_dir_path = raw_dir_path
        self.url = url
        self.closed = 0

    def tracking(self):
        if self.closed == 0:
            threading.Timer(5.0*60, self.tracking).start()
        yield FormRequest(self.url, method='POST', callback=None, formdata=self.crawler.stats.get_stats())

    def parse(self, response):

        with open(self.raw_dir_path + '/raw_state.txt', 'a') as rawFile:
            rawFile.write(str(response.body))
        rawFile.close()
        
        fieldNames = ['State', 'No. Of Applications Received', 'No. Of Applications not Verified', \
		'No. Of Applications Verified', 'No. of Applications Approved','No. of Applications Approved having Aadhar No.', \
		'No. of Applications Rejected','No. of Applications Pullback','No. of Applications Closed','No. of Constructed Toilet Photo', \
		'No. of Commenced Toilet Photo','No. of Constructed Toilet Photo through Swachhalaya']
        with open(self.data_dir_path + '/state_data.csv', 'w') as stateFile:
            csvWriter = csv.writer(stateFile)
            csvWriter.writerow(fieldNames)
        stateFile.close()
        
        fieldNames = ['State','District', 'No. Of Applications Received', 'No. Of Applications not Verified', \
		'No. Of Applications Verified', 'No. of Applications Approved','No. of Applications Approved having Aadhar No.', \
		'No. of Applications Rejected','No. of Applications Pullback','No. of Applications Closed','No. of Constructed Toilet Photo', \
		'No. of Commenced Toilet Photo','No. of Constructed Toilet Photo through Swachhalaya']
        with open(self.data_dir_path + '/district_data.csv', 'w') as districtFile:
            csvWriter = csv.writer(districtFile)
            csvWriter.writerow(fieldNames)
        districtFile.close()
        
        fieldNames = ['State','District','ULB Name', 'No. Of Applications Received', 'No. Of Applications not Verified', \
		'No. Of Applications Verified', 'No. of Applications Approved','No. of Applications Approved having Aadhar No.', \
		'No. of Applications Rejected','No. of Applications Pullback','No. of Applications Closed','No. of Constructed Toilet Photo', \
		'No. of Commenced Toilet Photo','No. of Constructed Toilet Photo through Swachhalaya']
        with open(self.data_dir_path + '/ULB_data.csv', 'w') as ULBFile:
            csvWriter = csv.writer(ULBFile)
            csvWriter.writerow(fieldNames)
        ULBFile.close()

        fieldNames = ['State','District','ULB Name', 'Ward', 'No. Of Applications Received', 'No. Of Applications not Verified', \
		'No. Of Applications Verified', 'No. of Applications Approved','No. of Applications Approved having Aadhar No.', \
		'No. of Applications Rejected','No. of Applications Pullback','No. of Applications Closed','No. of Constructed Toilet Photo', \
		'No. of Commenced Toilet Photo','No. of Constructed Toilet Photo through Swachhalaya']
        with open(self.data_dir_path + '/Ward_data.csv', 'w') as WardFile:
            csvWriter = csv.writer(WardFile)
            csvWriter.writerow(fieldNames)
        WardFile.close()

        if self.url != None:
            self.tracking()

        state = 1

        for row in Selector(response).xpath('//table[@id="ContentPlaceHolder1_gvApplicationListState"]/tr[not(contains(@class,"GridViewHeader"))]'):
            field = {}
            field['State'] = row.xpath('normalize-space(td[2])').extract()
            field['No_of_Applications_Received'] = row.xpath('normalize-space(td[3])').extract()
            field['No_of_Applications_Not_Verified'] = row.xpath('normalize-space(td[4])').extract()
            field['No_of_Applications_Verified'] = row.xpath('normalize-space(td[5])').extract()
            field['No_of_Applications_Approved'] = row.xpath('normalize-space(td[6])').extract()
            field['No_of_Applications_With_Aadhar'] = row.xpath('normalize-space(td[7])').extract()
            field['No_of_Applications_Rejected'] = row.xpath('normalize-space(td[8])').extract()
            field['No_of_Applications_PullBack'] = row.xpath('normalize-space(td[9])').extract()
            field['No_of_Applications_Closed'] = row.xpath('normalize-space(td[10])').extract()
            field['No_of_Constructed_Toilet_Photo'] = row.xpath('normalize-space(td[11])').extract()
            field['No_of_Commenced_Toilet_Photo'] = row.xpath('normalize-space(td[12])').extract()
            field['No_of_Constructed_Toilet_Photo_through_Swachhalaya'] = row.xpath('normalize-space(td[13])').extract()
            fs = field['State']
            for key in field.keys():
                field[key] = field[key][0].rstrip()

            with open(self.data_dir_path + '/state_data.csv', 'a') as stateFile:
                csvWriter = csv.writer(stateFile)
                csvWriter.writerow([field['State'],field['No_of_Applications_Received'],field['No_of_Applications_Not_Verified'],field['No_of_Applications_Verified'],field['No_of_Applications_Approved'],field['No_of_Applications_With_Aadhar'],field['No_of_Applications_Rejected'],field['No_of_Applications_PullBack'],field['No_of_Applications_Closed'],field['No_of_Constructed_Toilet_Photo'],field['No_of_Commenced_Toilet_Photo'],field['No_of_Constructed_Toilet_Photo_through_Swachhalaya']])
                stateFile.close()

            data = {}
            state_string=str(state+1).zfill(2)
            data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$gvApplicationListState$ctl'+state_string+'$lnkSTATE_NAME'
            data['__VIEWSTATE'] = response.xpath("//input[@id = '__VIEWSTATE']/@value").extract_first()
            data['__VIEWSTATEGENERATOR'] = '94FEA955'
            data['__VIEWSTATEENCRYPTED'] = ''
            yield FormRequest(url="http://swachhbharaturban.gov.in/ihhl/RPTApplicationSummary.aspx/",method='POST',callback=self.district,formdata=data,dont_filter=True,meta={'stateName':fs})

            state += 1

        self.closed = 1

    def district(self, response):
        
        with open(self.raw_dir_path + '/raw_district.txt', 'a') as rawFile:
            rawFile.write(str(response.body))
        rawFile.close()
        
        district = 1
        for row in Selector(response).xpath('//table[@id="ContentPlaceHolder1_gvApplicationListDistrict"]/tr[not(contains(@class,"GridViewHeader"))]'):
            field = {}
            field['State'] = response.meta['stateName']
            field['District'] = row.xpath('normalize-space(td[2])').extract()
            field['No_of_Applications_Received'] = row.xpath('normalize-space(td[3])').extract()
            field['No_of_Applications_Not_Verified'] = row.xpath('normalize-space(td[4])').extract()
            field['No_of_Applications_Verified'] = row.xpath('normalize-space(td[5])').extract()
            field['No_of_Applications_Approved'] = row.xpath('normalize-space(td[6])').extract()
            field['No_of_Applications_With_Aadhar'] = row.xpath('normalize-space(td[7])').extract()
            field['No_of_Applications_Rejected'] = row.xpath('normalize-space(td[8])').extract()
            field['No_of_Applications_PullBack'] = row.xpath('normalize-space(td[9])').extract()
            field['No_of_Applications_Closed'] = row.xpath('normalize-space(td[10])').extract()
            field['No_of_Constructed_Toilet_Photo'] = row.xpath('normalize-space(td[11])').extract()
            field['No_of_Commenced_Toilet_Photo'] = row.xpath('normalize-space(td[12])').extract()
            field['No_of_Constructed_Toilet_Photo_through_Swachhalaya'] = row.xpath('normalize-space(td[13])').extract()
            fd = field['District']
            for key in field.keys():
                field[key] = field[key][0].rstrip()
            with open(self.data_dir_path + '/district_data.csv', 'a') as districtFile:
                csvWriter = csv.writer(districtFile)
                csvWriter.writerow([field['State'],field['District'],field['No_of_Applications_Received'],field['No_of_Applications_Not_Verified'],field['No_of_Applications_Verified'],field['No_of_Applications_Approved'],field['No_of_Applications_With_Aadhar'],field['No_of_Applications_Rejected'],field['No_of_Applications_PullBack'],field['No_of_Applications_Closed'],field['No_of_Constructed_Toilet_Photo'],field['No_of_Commenced_Toilet_Photo'],field['No_of_Constructed_Toilet_Photo_through_Swachhalaya']])
                districtFile.close()
                
            data = {}
            district_string=str(district+1).zfill(2)
            data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$gvApplicationListDistrict$ctl'+district_string+'$LinkButton2'
            data['__VIEWSTATE'] = response.xpath("//input[@id = '__VIEWSTATE']/@value").extract_first()
            data['__VIEWSTATEGENERATOR'] = '94FEA955'
            data['__VIEWSTATEENCRYPTED'] = ''
            yield FormRequest(url="http://swachhbharaturban.gov.in/ihhl/RPTApplicationSummary.aspx/",method='POST',callback=self.ulb,formdata=data,dont_filter=True,meta={'stateName':response.meta['stateName'], 'districtName':fd})

            district += 1

	
    def ulb(self, response):

        with open(self.raw_dir_path + '/raw_ulb.txt', 'a') as rawFile:
            rawFile.write(str(response.body))
        rawFile.close()
        
        ulb = 1
        for row in Selector(response).xpath('//table[@id="ContentPlaceHolder1_gvApplicationListULB"]/tr[not(contains(@class,"GridViewHeader"))]'):
            field = {}
            field['State'] = response.meta['stateName']
            field['District'] = response.meta['districtName']
            field['ULB_Name'] = row.xpath('normalize-space(td[2])').extract()
            field['No_of_Applications_Received'] = row.xpath('normalize-space(td[3])').extract()
            field['No_of_Applications_Not_Verified'] = row.xpath('normalize-space(td[4])').extract()
            field['No_of_Applications_Verified'] = row.xpath('normalize-space(td[5])').extract()
            field['No_of_Applications_Approved'] = row.xpath('normalize-space(td[6])').extract()
            field['No_of_Applications_With_Aadhar'] = row.xpath('normalize-space(td[7])').extract()
            field['No_of_Applications_Rejected'] = row.xpath('normalize-space(td[8])').extract()
            field['No_of_Applications_PullBack'] = row.xpath('normalize-space(td[9])').extract()
            field['No_of_Applications_Closed'] = row.xpath('normalize-space(td[10])').extract()
            field['No_of_Constructed_Toilet_Photo'] = row.xpath('normalize-space(td[11])').extract()
            field['No_of_Commenced_Toilet_Photo'] = row.xpath('normalize-space(td[12])').extract()
            field['No_of_Constructed_Toilet_Photo_through_Swachhalaya'] = row.xpath('normalize-space(td[13])').extract()
            fu = field['ULB_Name']
            for key in field.keys():
                field[key] = field[key][0].rstrip()
            with open(self.data_dir_path + '/ULB_data.csv', 'a') as ULBFile:
                csvWriter = csv.writer(ULBFile)
                csvWriter.writerow([field['State'],field['District'],field['ULB_Name'],field['No_of_Applications_Received'],field['No_of_Applications_Not_Verified'],field['No_of_Applications_Verified'],field['No_of_Applications_Approved'],field['No_of_Applications_With_Aadhar'],field['No_of_Applications_Rejected'],field['No_of_Applications_PullBack'],field['No_of_Applications_Closed'],field['No_of_Constructed_Toilet_Photo'],field['No_of_Commenced_Toilet_Photo'],field['No_of_Constructed_Toilet_Photo_through_Swachhalaya']])
                ULBFile.close()
            data = {}
            ulb_string=str(ulb+1).zfill(2)
            data['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder1$gvApplicationListULB$ctl'+ulb_string+'$LinkButton3'
            data['__VIEWSTATE'] = response.xpath("//input[@id = '__VIEWSTATE']/@value").extract_first()
            data['__VIEWSTATEGENERATOR'] = '94FEA955'
            data['__VIEWSTATEENCRYPTED'] = ''
            yield FormRequest(url="http://swachhbharaturban.gov.in/ihhl/RPTApplicationSummary.aspx/",method='POST',callback=self.ward,formdata=data,dont_filter=True,meta={'stateName':response.meta['stateName'], 'districtName':response.meta['districtName'],'ULB_Name':fu})

            ulb += 1

    def ward(self, response):

        with open(self.raw_dir_path + '/raw_ward.txt', 'a') as rawFile:
            rawFile.write(str(response.body))
        rawFile.close()
        
        for row in Selector(response).xpath('//table[@id="ContentPlaceHolder1_gvApplicationListWARD"]/tr[not(contains(@class,"GridViewHeader"))]'):
            field = {}
            field['State'] = response.meta['stateName']
            field['District'] = response.meta['districtName']
            field['ULB_Name'] = response.meta['ULB_Name']
            field['Ward'] = row.xpath('normalize-space(td[2])').extract()
            field['No_of_Applications_Received'] = row.xpath('normalize-space(td[3])').extract()
            field['No_of_Applications_Not_Verified'] = row.xpath('normalize-space(td[4])').extract()
            field['No_of_Applications_Verified'] = row.xpath('normalize-space(td[5])').extract()
            field['No_of_Applications_Approved'] = row.xpath('normalize-space(td[6])').extract()
            field['No_of_Applications_With_Aadhar'] = row.xpath('normalize-space(td[7])').extract()
            field['No_of_Applications_Rejected'] = row.xpath('normalize-space(td[8])').extract()
            field['No_of_Applications_PullBack'] = row.xpath('normalize-space(td[9])').extract()
            field['No_of_Applications_Closed'] = row.xpath('normalize-space(td[10])').extract()
            field['No_of_Constructed_Toilet_Photo'] = row.xpath('normalize-space(td[11])').extract()
            field['No_of_Commenced_Toilet_Photo'] = row.xpath('normalize-space(td[12])').extract()
            field['No_of_Constructed_Toilet_Photo_through_Swachhalaya'] = row.xpath('normalize-space(td[13])').extract()
            for key in field.keys():
                field[key] = field[key][0].rstrip()
            with open(self.data_dir_path + '/Ward_data.csv', 'a') as WardFile:
                csvWriter = csv.writer(WardFile)
                csvWriter.writerow([field['State'],field['District'],field['ULB_Name'],field['Ward'],field['No_of_Applications_Received'],field['No_of_Applications_Not_Verified'],field['No_of_Applications_Verified'],field['No_of_Applications_Approved'],field['No_of_Applications_With_Aadhar'],field['No_of_Applications_Rejected'],field['No_of_Applications_PullBack'],field['No_of_Applications_Closed'],field['No_of_Constructed_Toilet_Photo'],field['No_of_Commenced_Toilet_Photo'],field['No_of_Constructed_Toilet_Photo_through_Swachhalaya']])
                WardFile.close()
