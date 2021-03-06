/* eslint-disable import/no-unresolved */
import React from 'react';
import PropTypes from 'prop-types';
import MainBody from './MainBody';

import AssignmentContext from './assignment-context';
import './styles.scss';

export default class AssignmentsEdit extends React.Component {
  state = {
    data: {
      assignmentName: this.props.inputValues.assignmentName,
      startDate: this.props.inputValues.startDate,
      endDate: this.props.inputValues.toDate,
    },
  };

  // event handlers
  onChangeState = ({ target }) => {
    this.setState(state => ({
      data: { ...state.data, [target.id]: target.value },
    }));
  };

  onChangeDate = (date, id) => {
    this.setState(state => ({
      data: {
        ...state.data,
        [id]: date,
      },
    }));
  };

  onSubmit = () => {
    const { assignmentName, startDate, endDate } = this.state.data;

    this.props.actions.onSubmit({
      assignmentName,
      startDate,
      endDate,
    });
  };

  // renders
  renderTopBar() {
    const { strings, actions } = this.props;

    return (
      <div className="assignment-topbar">
        <div className="assignment-name">
          <button type="button" className="go-back" onClick={actions.cancelEditSurveyAssignment}>
            <i className="fa fa-arrow-left" />
          </button>

          <h3>
            <input
              type="text"
              id="assignmentName"
              placeholder={strings.assignmentNamePlaceholder}
              value={this.state.data.assignmentName}
              onChange={this.onChangeState}
            />
          </h3>
        </div>

        <button onClick={this.onSubmit} type="button" className="standardBtn">
          {strings.saveAssignment}
        </button>
      </div>
    );
  }

  render() {
    const contextData = {
      strings: this.props.strings,
      actions: {
        ...this.props.actions,
        onInputChange: this.onChangeState,
        onDateChange: this.onChangeDate,
      },
      data: this.props.data,
      inputValues: this.state.data,
    };

    return (
      <div className="assignments-edit">
        {/* topbar */}
        {this.renderTopBar()}

        <AssignmentContext.Provider value={contextData}>
          <MainBody />
        </AssignmentContext.Provider>
      </div>
    );
  }
}

AssignmentsEdit.propTypes = {
  strings: PropTypes.object.isRequired,
  actions: PropTypes.object.isRequired,
  inputValues: PropTypes.object.isRequired,
  data: PropTypes.object.isRequired,
};
