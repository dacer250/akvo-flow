package org.waterforpeople.mapping.app.gwt.client.survey;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.waterforpeople.mapping.app.gwt.client.survey.QuestionDto.QuestionType;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

@RemoteServiceRelativePath("surveyrpcservice")
public interface SurveyService extends RemoteService {

	public static final String DATE_ROLL_UP = "collectionDate";
	// TODO: change this to the region field name once it's added
	public static final String REGION_ROLL_UP = "collectionDate";

	public SurveyDto[] listSurvey();

	public QuestionDto[] listSurveyQuestionByType(Long surveyId, QuestionType type);

	public ArrayList<SurveyGroupDto> listSurveyGroups(String cursorString,
			Boolean loadSurveyFlag, Boolean loadQuestionGroupFlag,
			Boolean loadQuestionFlag);

	/**
	 * lists all surveys for a group
	 */
	public ArrayList<SurveyDto> listSurveysByGroup(String surveyGroupId);

	public ArrayList<QuestionGroupDto> listQuestionGroupsBySurvey(
			String surveyId);

	public ArrayList<QuestionDto> listQuestionsByQuestionGroup(
			String questionGroupId, boolean needDetails);

	public SurveyGroupDto save(SurveyGroupDto value);

	/**
	 * fully hydrates a survey object
	 * 
	 * @param surveyId
	 * @return
	 */
	public SurveyDto loadFullSurvey(Long surveyId);

	public List<SurveyDto> listSurveysForSurveyGroup(String surveyGroupCode);
	
	public QuestionDto loadQuestionDetails(Long questionId);

	
	public QuestionDto saveQuestion(QuestionDto value, Long questionGroupId);
	public String deleteSurveyGroup(SurveyGroupDto value);
	public String deleteSurvey(SurveyDto value, Long surveyGroupId);
	public String deleteQuestionGroup(QuestionGroupDto value, Long surveyId);
	public String deleteQuestion(QuestionDto value, Long questionGroupId);
	public SurveyDto saveSurvey(SurveyDto surveyDto, Long surveyGroupId);
	public QuestionGroupDto saveQuestionGroup(QuestionGroupDto dto, Long surveyId);
	public SurveyGroupDto saveSurveyGroup(SurveyGroupDto dto);
	public String publishSurvey(Long surveyId);
	public void publishSurveyAsync(Long surveyId);
	public List<TranslationDto> saveTranslations(List<TranslationDto> translations);
	public void rerunAPMappings(Long surveyId);
	public List<QuestionHelpDto> listHelpByQuestion(Long questionId);
	public List<QuestionHelpDto> saveHelp(List<QuestionHelpDto> helpList);
	public Map<String,TranslationDto> listTranslations(Long parentId, String parentType);
	public List<QuestionGroupDto> saveQuestionGroups(List<QuestionGroupDto> dtoList);
	public QuestionDto copyQuestion(QuestionDto existingQuestion,
			QuestionGroupDto newParentGroup);
	public void updateQuestionOrder(List<QuestionDto> questions);
	public void updateQuestionGroupOrder(List<QuestionGroupDto> groups);
	public void updateQuestionDependency(Long questionId,
			QuestionDependencyDto dep);
}
