import isArray from 'lodash/isArray';
import isString from 'lodash/isString';

export default mappings => Ember.Mixin.create({
  init() {
    this._super();
    Object.keys(mappings).forEach((key) => {
      if (!key || (isString(key) && !key.length)) {
        throw new Error('invalid observer key');
      }
      const handler = mappings[key];
      if (!handler || (isString(handler) && !handler.length)) {
        throw new Error('observer method not defined');
      }
      (isArray(handler) ? handler : [handler]).forEach((h) => {
        this.addObserver(key, this, h);
      });
    });
  },
});
