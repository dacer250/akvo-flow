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

import java.util.List;

import org.waterforpeople.mapping.app.web.dto.SurveyedLocaleDto;

import com.gallatinsystems.framework.rest.RestResponse;

/**
 * response for DataPointServlet service
 *
 */
public class DataPointResponse extends RestResponse {
    private static final long serialVersionUID = 1L;
    private List<SurveyedLocaleDto> dataPointData;

    public List<SurveyedLocaleDto> getDataPointData() {
        return dataPointData;
    }

    public void setDataPointData(List<SurveyedLocaleDto> dataPointData) {
        this.dataPointData = dataPointData;
    }

}
