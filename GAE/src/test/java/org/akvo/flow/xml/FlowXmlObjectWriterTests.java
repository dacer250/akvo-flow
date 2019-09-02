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

package test.java.org.akvo.flow.xml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

import org.akvo.flow.xml.PublishedForm;
import org.akvo.flow.xml.XmlForm;
import org.akvo.flow.xml.XmlQuestionGroup;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.waterforpeople.mapping.app.gwt.client.survey.SurveyDto;

import com.gallatinsystems.survey.domain.Question;
import com.gallatinsystems.survey.domain.QuestionGroup;
import com.gallatinsystems.survey.domain.Survey;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;


class FlowXmlObjectWriterTests {

    private final LocalServiceTestHelper helper =
            new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());

    @Before
    public void setUp() {
      helper.setUp();
    }

    @After
    public void tearDown() {
      helper.tearDown();
    }

    @Test
    void testSerialiseEmptyForm() throws IOException {
        //Mock up a DTO tree
        Survey form1 = new Survey();
        //TODO needs the helper (to register an API environment)?
        form1.setKey(KeyFactory.createKey("Survey", 17L));
        form1.setName("This is a form");
        form1.setVersion(10.0);
        //No question groups. Completely empty form.

        //Convert domain tree to Jackson tree
        XmlForm form = new XmlForm(form1);
        //...and test it
        assertNotEquals(null, form);
        //assertEquals(17L, form.getSurveyId());
        assertEquals("This is a form", form.getName());
        assertEquals("10.0", form.getVersion());
        assertNotEquals(null, form.getQuestionGroup());

        //Convert Jackson tree into an XML string
        String xml = PublishedForm.generate(form);

    }


    @Test
    void testSerialiseQuestionlessForm() throws IOException {

        //Mock up a form tree
        Survey form1 = new Survey();
        //form1.setKey(KeyFactory.createKey("Survey", 17L));
        form1.setName("This is a form");
        form1.setVersion(11.0);
        //Add a QuestionGroup
        QuestionGroup qg = new QuestionGroup();
        //qg.setKey(KeyFactory.createKey("Survey", 18L));
        qg.setSurveyId(17L);
        qg.setName("This is a group");
        qg.setOrder(1);
        TreeMap<Integer, QuestionGroup> gl = new TreeMap<>();
        gl.put(1, qg);
        form1.setQuestionGroupMap(gl);
        //No questions

        //Convert DTO tree to Jackson tree
        XmlForm form = new XmlForm(form1);
        //...and test it
        assertNotEquals(null, form);
//        assertEquals(17L, form.getSurveyId());
        assertEquals("This is a form", form.getName());
        assertEquals("11.0", form.getVersion());
        assertNotEquals(null, form.getQuestionGroup());
        List<XmlQuestionGroup> ga = form.getQuestionGroup();
        assertEquals(1, ga.size());
        assertEquals("This is a group", ga.get(0).getHeading());
        assertEquals(1, ga.get(0).getOrder());
        assertFalse(ga.get(0).getRepeatable());

        //Convert Jackson tree into an XML string
        String xml = PublishedForm.generate(form);

        //And finally back to DTO again
        SurveyDto dto2 = PublishedForm.parse(xml, true).toDto(); //be strict

        assertNotEquals(null, dto2);
//        assertEquals(17L, dto2.getKeyId());
        assertEquals("This is a form", dto2.getName());
        assertEquals("11.0", dto2.getVersion());
        assertEquals("This is a form", dto2.getName());
    }


    /* Now we add a question */
    @Test
    void testSerialiseMinimalForm() throws IOException {

        //Mock up a DTO tree
        Survey form1 = new Survey();
//        form1.setKey(KeyFactory.createKey("Survey", 17L));
        form1.setName("This is a form");
        form1.setVersion(12.0);
        //Add a QuestionGroup
        QuestionGroup qg = new QuestionGroup();
//        qg.setKey(KeyFactory.createKey("Survey", 18L));
        qg.setSurveyId(17L);
        qg.setName("This is a group");
        qg.setOrder(1);
        TreeMap<Integer, QuestionGroup> gl = new TreeMap<>();
        gl.put(1, qg);
        form1.setQuestionGroupMap(gl);
        //Add a question
        //Intentionally do not set mandatory; it should be null
        Question q = new Question();
//        q.setKey(KeyFactory.createKey("Survey", 19L)); //Must have a key
        q.setOrder(1);
        q.setText("This is a question");
        q.setType(Question.Type.FREE_TEXT);
        TreeMap<Integer,Question> qm = new TreeMap<>();
        qm.put(1,q);
        qg.setQuestionMap(qm);

        //Convert DTO tree to Jackson tree
        XmlForm form = new XmlForm(form1);
        //...and test it
        assertNotEquals(null, form);
        assertEquals(17L, form.getSurveyId());
        assertEquals("This is a form", form.getName());
        assertEquals("12.0", form.getVersion());
        assertNotEquals(null, form.getQuestionGroup());
        List<XmlQuestionGroup> ga = form.getQuestionGroup();
        assertEquals(1, ga.size());
        assertEquals("This is a group", ga.get(0).getHeading());
        assertEquals(1, ga.get(0).getOrder());
        assertFalse(ga.get(0).getRepeatable());
        //TODO test the question

        //Convert Jackson tree into an XML string
        String xml = PublishedForm.generate(form);

        //And finally back to DTO again
        //TODO: key is mandatory
        SurveyDto dto2 = PublishedForm.parse(xml, true).toDto(); //be strict

        assertNotEquals(null, dto2);
        assertEquals(17L, dto2.getKeyId());
        assertEquals("This is a form", dto2.getName());
        assertEquals("12.0", dto2.getVersion());
        assertEquals("This is a form", dto2.getName());

        //Exactly The same? (Platonic ideal - not required)
        //assertTrue(dto1.equals(dto2));
    }

}
