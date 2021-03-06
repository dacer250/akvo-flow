/*
 *  Copyright (C) 2014-2015,2017-2018,2020 Stichting Akvo (Akvo Foundation)
 *
 *  This file is part of Akvo FLOW.
 *
 *  Akvo FLOW is free software: you can redistribute it and modify it under the terms of
 *  the GNU Affero General Public License (AGPL) as published by the Free Software Foundation,
 *  either version 3 of the License or any later version.
 *
 *  Akvo FLOW is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Affero General Public License included below for more details.
 *
 *  The full license text can also be seen at <http://www.gnu.org/licenses/agpl.html>.
 */

package org.waterforpeople.mapping.app.web.rest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gallatinsystems.common.Constants;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.waterforpeople.mapping.app.util.DtoMarshaller;
import org.waterforpeople.mapping.app.web.dto.SurveyedLocaleDto;
import org.waterforpeople.mapping.app.web.rest.dto.RestStatusDto;
import com.gallatinsystems.surveyal.dao.SurveyedLocaleDao;
import com.gallatinsystems.surveyal.domain.SurveyedLocale;

@Controller
@RequestMapping("/surveyed_locales")
public class SurveyedLocaleRestService {

    private SurveyedLocaleDao surveyedLocaleDao = new SurveyedLocaleDao();

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public Map<String, Object> listDataPoints(
            @RequestParam(value = "surveyGroupId", defaultValue = "") Long surveyGroupId,
            @RequestParam(value = "identifier", defaultValue = "") String identifier,
            @RequestParam(value = "ids[]", defaultValue = "") Long[] ids,
            @RequestParam(value = "displayName", defaultValue = "") String displayName,
            @RequestParam(value = "displayNamePrefix", defaultValue = "") String displayNamePrefix,
            @RequestParam(value = "search", defaultValue = "") String search,
            @RequestParam(value = "since", defaultValue = "") String since) {

        Map<String, Object> response = new HashMap<String, Object>();

        RestStatusDto statusDto = new RestStatusDto();
        statusDto.setStatus("");
        statusDto.setMessage("");

        List<SurveyedLocale> sls = new ArrayList<SurveyedLocale>();
        List<SurveyedLocaleDto> locales = new ArrayList<SurveyedLocaleDto>();
        boolean searchIdentifiers = false;

        if (search != null && !"".equals(search) && surveyGroupId != null) {
            sls = surveyedLocaleDao.listSurveyedLocales(since, surveyGroupId, null, null, search);
            searchIdentifiers = search.matches(SurveyedLocale.IDENTIFIER_PATTERN); //could consider a2a2-a2a2-a2a too
        } else if (identifier != null && !"".equals(identifier)) {
            sls = surveyedLocaleDao.listSurveyedLocales(since, null, identifier, null, null);
        } else if (displayName != null && !"".equals(displayName)) {
            sls = surveyedLocaleDao.listSurveyedLocales(since, null, null, displayName, null);
        } else if (displayNamePrefix != null && !"".equals(displayNamePrefix)) {
            sls = surveyedLocaleDao.listSurveyedLocales(since, null, null, null, displayNamePrefix);
        } else if (surveyGroupId != null) {
            sls = surveyedLocaleDao.listSurveyedLocales(since, surveyGroupId, null, null, null);
        } else if (ids[0] != null) {
            sls = surveyedLocaleDao.listByKeys(ids);
        }

        copyToDtoList(sls, locales);

        if (searchIdentifiers) {
            //JDO implementation cannot handle both OR and a prefix in a filter expression,
            //so we have to search again and concatenate
            List<SurveyedLocale> sls2 = surveyedLocaleDao.listSurveyedLocales(null, surveyGroupId, search, null, null);
            copyToDtoList(sls2, locales);
        }

        Integer num = locales.size();
        String newSince = SurveyedLocaleDao.getCursor(sls);
        statusDto.setNum(num);
        statusDto.setSince(newSince);

        response.put("surveyed_locales", locales);
        response.put("meta", statusDto);
        return response;
    }

    @RequestMapping(method = RequestMethod.GET, value = "/{dataPointId}")
    @ResponseBody
    public Map<String, Object> findDataPointById(
            @PathVariable("dataPointId") Long dataPointId) {
        final Map<String, Object> response = new HashMap<>();
        final RestStatusDto statusDto = new RestStatusDto();
        statusDto.setStatus("");
        statusDto.setMessage("");

        SurveyedLocale dataPoint = surveyedLocaleDao.getByKey(dataPointId);
        SurveyedLocaleDto dto = null;
        if (dataPoint != null) {
            dto = new SurveyedLocaleDto();
            BeanUtils.copyProperties(dataPoint, dto, Constants.EXCLUDED_PROPERTIES);
            dto.setKeyId(dataPoint.getKey().getId());
        }
        response.put("surveyed_locale", dto);
        response.put("meta", statusDto);
        return response;
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "/{id}")
    @ResponseBody
    public Map<String, RestStatusDto> deleteSurveyedLocaleById(
            @PathVariable("id") Long id) {
        final Map<String, RestStatusDto> response = new HashMap<String, RestStatusDto>();
        SurveyedLocale sl = surveyedLocaleDao.getByKey(id);
        RestStatusDto statusDto = new RestStatusDto();
        statusDto.setStatus("failed");
        // check if surveyInstance exists in the datastore
        if (sl != null) {
            surveyedLocaleDao.deleteSurveyedLocale(sl);
            statusDto.setStatus("ok");
        }
        response.put("meta", statusDto);
        return response;
    }

    private void copyToDtoList(List<SurveyedLocale>list1, List<SurveyedLocaleDto> list2) {
        for (SurveyedLocale sl : list1) {
            SurveyedLocaleDto dto = new SurveyedLocaleDto();
            DtoMarshaller.copyToDto(sl, dto);
            list2.add(dto);
        }

    }
}
