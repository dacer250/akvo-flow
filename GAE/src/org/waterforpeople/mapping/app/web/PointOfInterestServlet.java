package org.waterforpeople.mapping.app.web;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONArray;
import org.json.JSONObject;
import org.waterforpeople.mapping.app.gwt.client.location.PointOfInterestDto;
import org.waterforpeople.mapping.app.web.dto.PointOfInterestRequest;
import org.waterforpeople.mapping.app.web.dto.PointOfInterestResponse;
import org.waterforpeople.mapping.dao.AccessPointDao;
import org.waterforpeople.mapping.domain.AccessPoint;

import com.gallatinsystems.framework.rest.AbstractRestApiServlet;
import com.gallatinsystems.framework.rest.RestRequest;
import com.gallatinsystems.framework.rest.RestResponse;
import com.gallatinsystems.gis.location.GeoLocationService;
import com.gallatinsystems.gis.location.GeoLocationServiceGeonamesImpl;

/**
 * JSON service for returning the list of points near a specific lat/lon point.
 * Points of interest can be access points, tasks or other domains that are
 * locationAware.
 * 
 * @author Christopher Fagiani
 */
public class PointOfInterestServlet extends AbstractRestApiServlet {

	private static final long serialVersionUID = 8748650927754433019L;
	private static final int MAX_DISTANCE_METERS = 10000;
	private AccessPointDao accessPointDao;
	private GeoLocationService geoService;

	public PointOfInterestServlet() {
		setMode(JSON_MODE);
		accessPointDao = new AccessPointDao();
		geoService = new GeoLocationServiceGeonamesImpl();
	}

	@Override
	protected RestRequest convertRequest() throws Exception {
		HttpServletRequest req = getRequest();
		RestRequest restRequest = new PointOfInterestRequest();
		restRequest.populateFromHttpRequest(req);
		return restRequest;
	}

	/**
	 * calls the accessPointDao to get the list of access points near the point
	 * passed in via the request
	 */
	@Override
	protected RestResponse handleRequest(RestRequest req) throws Exception {
		PointOfInterestRequest piReq = (PointOfInterestRequest) req;
		return convertToResponse(accessPointDao.listNearbyAccessPoints(piReq
				.getLat(), piReq.getLon(), geoService.getCountryCodeForPoint(
				piReq.getLat().toString(), piReq.getLon().toString()),
				MAX_DISTANCE_METERS));
	}

	/**
	 * converts the domain objects to dtos and then installs them in an
	 * AccessPointResponse object
	 */
	protected PointOfInterestResponse convertToResponse(List<AccessPoint> apList) {
		PointOfInterestResponse resp = new PointOfInterestResponse();
		if (apList != null) {
			List<PointOfInterestDto> dtoList = new ArrayList<PointOfInterestDto>();
			for (AccessPoint ap : apList) {
				PointOfInterestDto dto = new PointOfInterestDto();
				dto.setName(ap.getCommunityCode());
				dto.setType(ap.getPointType() != null ? ap.getPointType()
						.toString() : "AccessPoint");
				dto.setLatitude(ap.getLatitude());
				dto.setLongitude(ap.getLongitude());
				dto.addProperty("status", ap.getPointStatus() != null ? ap
						.getPointStatus().toString() : "unknown");
				dtoList.add(dto);
			}
			resp.setPointsOfInterest(dtoList);
		}
		return resp;
	}

	/**
	 * writes response as a JSON string
	 */
	@Override
	protected void writeOkResponse(RestResponse resp) throws Exception {
		getResponse().setStatus(200);
		PointOfInterestResponse piResp = (PointOfInterestResponse) resp;
		JSONObject result = new JSONObject(piResp);
		JSONArray arr = result.getJSONArray("pointsOfInterest");
		if (arr != null) {
			for (int i = 0; i < arr.length(); i++) {
				((JSONObject) arr.get(i)).put("propertyValues", piResp
						.getPointsOfInterest().get(i).getPropertyValues());

				((JSONObject) arr.get(i)).put("propertyNames", piResp
						.getPointsOfInterest().get(i).getPropertyNames());
			}
		}
		getResponse().getWriter().println(result.toString());
	}
}
