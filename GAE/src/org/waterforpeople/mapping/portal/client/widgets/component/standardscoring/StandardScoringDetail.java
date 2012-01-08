/**
 * 
 */
package org.waterforpeople.mapping.portal.client.widgets.component.standardscoring;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.waterforpeople.mapping.app.gwt.client.accesspoint.AccessPointManagerService;
import org.waterforpeople.mapping.app.gwt.client.accesspoint.AccessPointManagerServiceAsync;
import org.waterforpeople.mapping.app.gwt.client.community.CommunityService;
import org.waterforpeople.mapping.app.gwt.client.community.CommunityServiceAsync;
import org.waterforpeople.mapping.app.gwt.client.community.CountryDto;
import org.waterforpeople.mapping.app.gwt.client.standardscoring.StandardScoringManagerService;
import org.waterforpeople.mapping.app.gwt.client.standardscoring.StandardScoringManagerServiceAsync;
import org.waterforpeople.mapping.app.gwt.client.util.TextConstants;

import com.gallatinsystems.framework.gwt.util.client.MessageDialog;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.uibinder.client.UiHandler;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.HasText;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.Widget;

/**
 * @author dru
 * 
 */
public class StandardScoringDetail extends Composite implements HasText {
	private static TextConstants TEXT_CONSTANTS = GWT
			.create(TextConstants.class);
	private StandardScoringManagerServiceAsync svc;
	private CommunityServiceAsync communitySvc;
	private AccessPointManagerServiceAsync apSvc;
	private static StandardScoringDetailUiBinder uiBinder = GWT
			.create(StandardScoringDetailUiBinder.class);

	interface StandardScoringDetailUiBinder extends
			UiBinder<Widget, StandardScoringDetail> {
	}

	/**
	 * Because this class has a default constructor, it can be used as a binder
	 * template. In other words, it can be used in other *.ui.xml files as
	 * follows: <ui:UiBinder xmlns:ui="urn:ui:com.google.gwt.uibinder"
	 * xmlns:g="urn:import:**user's package**">
	 * <g:**UserClassName**>Hello!</g:**UserClassName> </ui:UiBinder> Note that
	 * depending on the widget that is used, it may be necessary to implement
	 * HasHTML instead of HasText.
	 */
	public StandardScoringDetail() {
		svc = GWT.create(StandardScoringManagerService.class);
		apSvc = GWT.create(AccessPointManagerService.class);
		communitySvc = GWT.create(CommunityService.class);
		initWidget(uiBinder.createAndBindUi(this));
		lbGlobal.addItem("Global");
		lbGlobal.addItem("Local");
		loadAttributes();
	}

	@UiField
	ListBox lbGlobal;
	@UiField
	ListBox lbCountry;
	@UiField
	Label labelCountry;
	@UiField
	ListBox evaluateField;

	@UiHandler("lbGlobal")
	void uiGlobalChange(ChangeEvent e) {

		if (((ListBox) e.getSource()).getItemText(
				((ListBox) e.getSource()).getSelectedIndex()).equalsIgnoreCase(
				"LOCAL")) {
			loadCountries();
			labelCountry.setVisible(true);
			lbCountry.setVisible(true);
		} else {
			labelCountry.setVisible(false);
			lbCountry.setVisible(false);
		}
	}

	@Override
	public String getText() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setText(String text) {
		// TODO Auto-generated method stub

	}

	private ArrayList<String> loadAttributes() {
		svc.listObjectAttributes(
				"org.waterforpeople.mapping.domain.AccessPoint",
				new AsyncCallback<TreeMap<String, String>>() {

					@Override
					public void onFailure(Throwable caught) {
						MessageDialog errDialog = new MessageDialog(
								TEXT_CONSTANTS.error(), TEXT_CONSTANTS
										.errorTracePrefix()
										+ " "
										+ caught.getLocalizedMessage());
						errDialog.showCentered();
					}

					@Override
					public void onSuccess(TreeMap<String, String> result) {
						for (Map.Entry<String, String> item : result.entrySet()) {
							evaluateField.addItem(item.getKey(),
									item.getValue());
						}
					}
				});

		return null;
	}

	private void loadCountries() {
		communitySvc.listCountries(new AsyncCallback<CountryDto[]>() {

			@Override
			public void onFailure(Throwable caught) {
				// TODO Auto-generated method stub

			}

			@Override
			public void onSuccess(CountryDto[] result) {
				for(CountryDto item:result){
					lbCountry.addItem(item.getIsoAlpha2Code(), item.getName());
				}
			}
		});
	}
}