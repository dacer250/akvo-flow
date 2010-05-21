package com.gallatinsystems.survey.dao;

import java.util.ArrayList;
import java.util.List;

import com.gallatinsystems.framework.dao.BaseDAO;
import com.gallatinsystems.survey.domain.OptionContainer;
import com.gallatinsystems.survey.domain.Question;
import com.gallatinsystems.survey.domain.QuestionOption;
import com.gallatinsystems.survey.domain.QuestionQuestionGroupAssoc;

public class QuestionDao extends BaseDAO<Question> {

	public QuestionDao(Class<Question> e) {
		super(e);
		// TODO Auto-generated constructor stub
	}

	public QuestionDao() {
		super(Question.class);
	}

	public List<Question> listQuestionsByQuestionGroup(String questionGroupCode) {
		List<QuestionQuestionGroupAssoc> qqgaList = new QuestionQuestionGroupAssocDao()
				.listByQuestionGroupId(new Long(questionGroupCode));
		java.util.ArrayList<Question> questionList = new ArrayList<Question>();

		for (QuestionQuestionGroupAssoc qqga : qqgaList) {
			Question question = getByKey(qqga.getQuestionId());
			setOptionContainer(question);
			questionList.add(question);
		}

		return questionList;
	}

	public void delete(Question question, Long questionGroupId) {
		QuestionQuestionGroupAssocDao qqgaDao = new QuestionQuestionGroupAssocDao();
		List<QuestionQuestionGroupAssoc> qqga = qqgaDao
				.listByQuestionId(question.getKey().getId());
		for (QuestionQuestionGroupAssoc item : qqga) {
			if (item.getQuestionGroupId().equals(questionGroupId))
				qqgaDao.delete(item);
		}
		question = super.getByKey(question.getKey().getId());
		super.delete(question);

	}

	public Question save(Question question, Long questionGroupId) {
		question = super.save(question);
		QuestionQuestionGroupAssoc qqga = new QuestionQuestionGroupAssoc();
		qqga.setQuestionGroupId(questionGroupId);
		qqga.setQuestionId(question.getKey().getId());
		QuestionQuestionGroupAssocDao qqgaDao = new QuestionQuestionGroupAssocDao();
		qqgaDao.save(qqga);

		if (question.getOptionContainer() != null) {
			OptionContainerDao ocDao = new OptionContainerDao();
			question.getOptionContainer().setQuestionId(question.getKey().getId());
			OptionContainer oc = ocDao.save(question.getOptionContainer());
			question.setOptionContainer(oc);
		}

		return question;
	}

	public Question save(Question question) {
		question = super.save(question);

		if (question.getOptionContainer() != null) {
			OptionContainerDao ocDao = new OptionContainerDao();
			question.getOptionContainer().setQuestionId(
					question.getKey().getId());
			OptionContainer oc = ocDao.save(question.getOptionContainer());
			question.setOptionContainer(oc);
		}

		return question;
	}

	public Question getByKey(Long id) {
		Question q = super.getByKey(id);

		return q;
	}

	private void setOptionContainer(Question question) {
		OptionContainerDao ocDao = new OptionContainerDao();
		OptionContainer oc = ocDao.findByQuestionId(question.getKey().getId());
		if (oc != null)
			question.setOptionContainer(oc);
	}

}
