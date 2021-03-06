/*
 *  Copyright (C) 2019 Stichting Akvo (Akvo Foundation)
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

package org.akvo.flow.api.app;

import com.gallatinsystems.device.dao.DeviceDAO;
import com.gallatinsystems.device.domain.Device;
import com.gallatinsystems.framework.rest.AbstractRestApiServlet;
import com.gallatinsystems.framework.rest.RestRequest;
import com.gallatinsystems.framework.rest.RestResponse;
import com.gallatinsystems.surveyal.dao.SurveyedLocaleDao;
import com.gallatinsystems.surveyal.domain.SurveyedLocale;
import org.akvo.flow.api.app.DataPointServlet;
import org.akvo.flow.dao.DataPointAssignmentDao;
import org.akvo.flow.domain.persistent.DataPointAssignment;
import org.akvo.flow.util.FlowJsonObjectWriter;
import org.waterforpeople.mapping.app.web.dto.SurveyedLocaleDto;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;


/**
 * JSON service for returning the list of assigned data point records for a specific device and surveyId
 */
@SuppressWarnings("serial")
public class DataPointServlet extends AbstractRestApiServlet {
    private static final Logger log = Logger.getLogger(DataPointServlet.class.getName());
    private SurveyedLocaleDao surveyedLocaleDao;
    private DataPointAssignmentDao dataPointAssignmentDao;
    private static Set<Long> ALL_DATAPOINTS = new HashSet<>(Arrays.asList(0L));

    public DataPointServlet() {
        setMode(JSON_MODE);
        surveyedLocaleDao = new SurveyedLocaleDao();
        dataPointAssignmentDao = new DataPointAssignmentDao();
    }

    @Override
    protected RestRequest convertRequest() throws Exception {
        HttpServletRequest req = getRequest();
        RestRequest restRequest = new DataPointRequest();
        restRequest.populateFromHttpRequest(req);
        return restRequest;
    }

    /**
     * calls the surveyedLocaleDao to get the list of surveyedLocales for a certain surveyGroupId
     * passed in via the request, or the total number of available surveyedLocales if the
     * checkAvailable flag is set.
     */
    @Override
    protected RestResponse handleRequest(RestRequest req) throws Exception {
        DataPointRequest dpReq = (DataPointRequest) req;
        List<SurveyedLocale> dpList;
        RestResponse res = new RestResponse();
        if (dpReq.getSurveyId() != null) {
            //Find the device (if any)
            DeviceDAO deviceDao = new DeviceDAO();
            Device device = deviceDao.getDevice(dpReq.getAndroidId(), null, null);
            if (device != null) {
                log.info("Found device id: " + device.getKey().getId());
                log.fine("Found device: " + device);
                //Find which assignments we are part of
                List<DataPointAssignment> assList =
                        dataPointAssignmentDao.listByDeviceAndSurvey(device.getKey().getId(), dpReq.getSurveyId());
                //Combine their point lists
                Set<Long> pointSet = new HashSet<>();
                for (DataPointAssignment ass : assList) {
                    pointSet.addAll(ass.getDataPointIds());
                }
                //Fetch the data points
                if (ALL_DATAPOINTS.equals(pointSet)) {
                    dpList = surveyedLocaleDao.listLocalesBySurveyGroupId(dpReq.getSurveyId());
                } else {
                    List<Long> pointList = new ArrayList<>();
                    pointList.addAll(pointSet);
                    dpList = surveyedLocaleDao.listByKeys(pointList);
                }
                res = convertToResponse(dpList, dpReq.getSurveyId());
                return res;
            }
            res.setCode(String.valueOf(HttpServletResponse.SC_NOT_FOUND));
            res.setMessage("Unknown device");
        } else {
            res.setCode(String.valueOf(HttpServletResponse.SC_FORBIDDEN));
            res.setMessage("Invalid Survey");
        }
        return res;
    }

    /**
     * converts the domain objects to dtos and then installs them in a DataPointResponse object
     */
    private DataPointResponse convertToResponse(List<SurveyedLocale> slList, Long surveyId) {
        DataPointResponse resp = new DataPointResponse();
        if (slList == null) {
            resp.setCode(String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR));
            resp.setMessage("Internal Server Error");
            return resp;
        }
        // set meta data
        resp.setCode(String.valueOf(HttpServletResponse.SC_OK));
        resp.setResultCount(slList.size());

        DataPointUtil dpu = new DataPointUtil();
        List<SurveyedLocaleDto> dtoList = dpu.getSurveyedLocaleDtosList(slList, surveyId);

        resp.setDataPointData(dtoList);
        return resp;
    }


    /**
     * writes response as a JSON string
     */
    @Override
    protected void writeOkResponse(RestResponse resp) throws Exception {
        int sc;
        try {
            sc = Integer.valueOf(resp.getCode());
        } catch (NumberFormatException ignored) {
            // Status code was not properly set in the RestResponse
            sc = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
        }
        getResponse().setStatus(sc);
        if (sc == HttpServletResponse.SC_OK) {
            FlowJsonObjectWriter writer = new FlowJsonObjectWriter();
            OutputStream stream = getResponse().getOutputStream();
            writer.writeValue(stream, resp);
            PrintWriter endwriter = new PrintWriter(stream);
            endwriter.println();
        } else {
            getResponse().getWriter().println(resp.getMessage());
        }
    }

}
